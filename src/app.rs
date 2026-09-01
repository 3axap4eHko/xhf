use std::io::{self, Read, Write};

use crate::cli::{
    AuthCommand, Cli, Command, RepoCommand, RepoListArgs, RepoTreeArgs, SearchCommand,
    SearchReposArgs,
};
use crate::config::{RuntimeContext, TokenSource, normalize_token};
use crate::download;
use crate::error::{AppError, AppResult};
use crate::hub::{
    HubClient, OrganizationRecord, RepositoryRecord, TOKEN_SETTINGS_URL, TreeEntry, UserRecord,
};
use crate::patterns::PatternSet;
use crate::text::escape_control;

pub fn run(
    cli: Cli,
    runtime: &RuntimeContext,
    output: &mut dyn Write,
    progress: &mut dyn Write,
    interactive_progress: bool,
) -> AppResult<()> {
    match cli.command {
        Command::Search { command } => run_search(command, runtime, output),
        Command::Repo { command } => run_repo(command, runtime, output),
        Command::Download(args) => {
            let client = client_from_credentials(runtime)?;
            download::execute(
                &client,
                &args,
                &runtime.current_directory,
                output,
                progress,
                interactive_progress,
            )
        }
        Command::Auth { command } => run_auth(command, runtime, output),
    }
}

fn run_search(
    command: SearchCommand,
    runtime: &RuntimeContext,
    output: &mut dyn Write,
) -> AppResult<()> {
    let client = client_from_credentials(runtime)?;
    match command {
        SearchCommand::Users(args) => {
            let records = client.search_users(&args.query, args.limit)?;
            write_users(output, &records, args.json)
        }
        SearchCommand::Orgs(args) => {
            let records = client.search_organizations(&args.query, args.limit)?;
            write_organizations(output, &records, args.json)
        }
        SearchCommand::Repos(args) => search_repositories(&client, &args, output),
    }
}

fn search_repositories(
    client: &HubClient,
    args: &SearchReposArgs,
    output: &mut dyn Write,
) -> AppResult<()> {
    if let Some(owner) = args.owner.as_deref() {
        validate_owner(owner)?;
    }
    let records = client.search_repositories(
        Some(&args.query),
        args.owner.as_deref(),
        args.repo_type,
        args.limit,
    )?;
    write_repositories(output, &records, args.json)
}

fn run_repo(
    command: RepoCommand,
    runtime: &RuntimeContext,
    output: &mut dyn Write,
) -> AppResult<()> {
    let client = client_from_credentials(runtime)?;
    match command {
        RepoCommand::List(args) => list_repositories(&client, &args, output),
        RepoCommand::Tree(args) => list_repository_tree(&client, &args, output),
        RepoCommand::Cat(args) => {
            let mut response =
                client.file_response(&args.repo, args.repo_type, &args.revision, &args.path)?;
            io::copy(&mut response, output).map_err(|error| {
                AppError::io(
                    format!("could not write {:?} to standard output", args.path),
                    error,
                )
            })?;
            Ok(())
        }
    }
}

fn list_repositories(
    client: &HubClient,
    args: &RepoListArgs,
    output: &mut dyn Write,
) -> AppResult<()> {
    let owner = match args.owner.as_deref() {
        Some(owner) => {
            validate_owner(owner)?;
            owner.to_owned()
        }
        None => client.whoami()?.name,
    };
    let records = client.search_repositories(None, Some(&owner), args.repo_type, args.limit)?;
    write_repositories(output, &records, args.json)
}

fn list_repository_tree(
    client: &HubClient,
    args: &RepoTreeArgs,
    output: &mut dyn Write,
) -> AppResult<()> {
    let entries = client.list_tree(&args.repo, args.repo_type, &args.revision)?;
    let patterns = PatternSet::compile(&args.patterns)?;
    let selected: Vec<&TreeEntry> = entries
        .iter()
        .filter(|entry| patterns.selects(&entry.path))
        .collect();
    if !args.patterns.is_empty() && selected.is_empty() {
        return Err(AppError::message("tree patterns matched no entries"));
    }
    if args.json {
        write_json(output, &selected)
    } else {
        for entry in selected {
            if entry.is_directory() {
                writeln!(output, "dir\t{}", escape_control(&entry.path))
                    .map_err(|error| AppError::io("could not write repository tree", error))?;
            } else {
                writeln!(
                    output,
                    "file\t{}\t{}",
                    entry.size,
                    escape_control(&entry.path)
                )
                .map_err(|error| AppError::io("could not write repository tree", error))?;
            }
        }
        Ok(())
    }
}

fn run_auth(
    command: AuthCommand,
    runtime: &RuntimeContext,
    output: &mut dyn Write,
) -> AppResult<()> {
    match command {
        AuthCommand::Login { with_token } => login(with_token, runtime, output),
        AuthCommand::Status => auth_status(runtime, output),
        AuthCommand::Logout => logout(runtime, output),
    }
}

fn login(with_token: bool, runtime: &RuntimeContext, output: &mut dyn Write) -> AppResult<()> {
    let token = read_login_token(with_token)?;
    let client = HubClient::new(Some(token.clone()))?;
    let identity = client.whoami()?;
    runtime.credentials.store(&token)?;
    writeln!(output, "Logged in as {}", escape_control(&identity.name))
        .map_err(|error| AppError::io("could not write authentication output", error))?;
    if runtime.credentials.environment_token_is_set() {
        writeln!(
            output,
            "HF_TOKEN remains active and overrides the stored token."
        )
        .map_err(|error| AppError::io("could not write authentication output", error))?;
    }
    Ok(())
}

fn auth_status(runtime: &RuntimeContext, output: &mut dyn Write) -> AppResult<()> {
    let Some((token, source)) = runtime.credentials.load()? else {
        return Err(AppError::message("not logged in"));
    };
    let client = HubClient::new(Some(token))?;
    let identity = client.whoami()?;
    let source = match source {
        TokenSource::Environment => "HF_TOKEN",
        TokenSource::File => "XDG token file",
    };
    writeln!(
        output,
        "{}\ntoken: {source}",
        escape_control(&identity.name)
    )
    .map_err(|error| AppError::io("could not write authentication status", error))?;
    if !identity.orgs.is_empty() {
        let organizations = identity
            .orgs
            .iter()
            .map(|organization| escape_control(&organization.name))
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(output, "orgs: {organizations}")
            .map_err(|error| AppError::io("could not write authentication status", error))?;
    }
    Ok(())
}

fn logout(runtime: &RuntimeContext, output: &mut dyn Write) -> AppResult<()> {
    let removed = runtime.credentials.remove_stored()?;
    let message = match (removed, runtime.credentials.environment_token_is_set()) {
        (true, true) => "Stored token removed; HF_TOKEN remains active.",
        (true, false) => "Stored token removed.",
        (false, true) => "No stored token; HF_TOKEN remains active.",
        (false, false) => "No stored token.",
    };
    writeln!(output, "{message}")
        .map_err(|error| AppError::io("could not write logout status", error))
}

fn read_login_token(with_token: bool) -> AppResult<String> {
    let token = if with_token {
        let mut token = String::new();
        io::stdin()
            .lock()
            .read_to_string(&mut token)
            .map_err(|error| AppError::io("could not read token from standard input", error))?;
        token
    } else {
        rpassword::prompt_password(login_prompt())
            .map_err(|error| AppError::io("could not read token from the terminal", error))?
    };
    normalize_token(token)
}

fn login_prompt() -> String {
    format!("Create a token: {TOKEN_SETTINGS_URL}\nHugging Face token: ")
}

fn client_from_credentials(runtime: &RuntimeContext) -> AppResult<HubClient> {
    let token = runtime.credentials.load()?.map(|(token, _)| token);
    HubClient::new(token)
}

fn validate_owner(owner: &str) -> AppResult<()> {
    if owner.is_empty() || owner.contains('/') {
        Err(AppError::message(
            "owner must be a single non-empty Hub namespace",
        ))
    } else {
        Ok(())
    }
}

fn write_users(output: &mut dyn Write, records: &[UserRecord], json: bool) -> AppResult<()> {
    if json {
        return write_json(output, records);
    }
    for record in records {
        write_name_record(output, &record.id, record.name.as_deref())?;
    }
    Ok(())
}

fn write_organizations(
    output: &mut dyn Write,
    records: &[OrganizationRecord],
    json: bool,
) -> AppResult<()> {
    if json {
        return write_json(output, records);
    }
    for record in records {
        write_name_record(output, &record.id, record.name.as_deref())?;
    }
    Ok(())
}

fn write_name_record(output: &mut dyn Write, id: &str, name: Option<&str>) -> AppResult<()> {
    let id = escape_control(id);
    match name.filter(|name| !name.is_empty()) {
        Some(name) => writeln!(output, "{id}\t{}", escape_control(name)),
        None => writeln!(output, "{id}"),
    }
    .map_err(|error| AppError::io("could not write search results", error))
}

fn write_repositories(
    output: &mut dyn Write,
    records: &[RepositoryRecord],
    json: bool,
) -> AppResult<()> {
    if json {
        return write_json(output, records);
    }
    for record in records {
        let visibility = if record.private { "private" } else { "public" };
        writeln!(
            output,
            "{}\t{}\t{visibility}",
            record.repo_type,
            escape_control(&record.id)
        )
        .map_err(|error| AppError::io("could not write repository results", error))?;
    }
    Ok(())
}

fn write_json<T: serde::Serialize + ?Sized>(output: &mut dyn Write, value: &T) -> AppResult<()> {
    serde_json::to_writer_pretty(&mut *output, value)
        .map_err(|error| AppError::json("could not serialize JSON output", error))?;
    writeln!(output).map_err(|error| AppError::io("could not terminate JSON output", error))
}

#[cfg(test)]
mod tests {
    use super::{login_prompt, write_name_record};
    use crate::hub::TOKEN_SETTINGS_URL;

    #[test]
    fn login_prompt_links_to_token_settings() {
        assert_eq!(
            login_prompt(),
            format!("Create a token: {TOKEN_SETTINGS_URL}\nHugging Face token: ")
        );
    }

    #[test]
    fn name_records_omit_empty_display_names() {
        let mut output = Vec::new();
        write_name_record(&mut output, "owner", Some("")).unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), "owner\n");
    }
}
