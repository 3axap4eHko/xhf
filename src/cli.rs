use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(
    name = "xhf",
    version,
    about = "Explore and download Hugging Face Hub content"
)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Search Hub users, organizations, and repositories.
    Search {
        #[command(subcommand)]
        command: SearchCommand,
    },
    /// List repositories and inspect repository contents.
    Repo {
        #[command(subcommand)]
        command: RepoCommand,
    },
    /// Download files selected by exact paths or repository-relative globs.
    #[command(
        after_help = "Patterns form a union. Prefix a quoted pattern with ! to subtract matches. With no patterns, every file is selected."
    )]
    Download(DownloadArgs),
    /// Manage the stored Hugging Face token.
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
}

#[derive(Debug, Subcommand)]
pub enum SearchCommand {
    /// Search user accounts.
    Users(SearchPeopleArgs),
    /// Search organizations.
    Orgs(SearchPeopleArgs),
    /// Search model, dataset, and Space repositories.
    Repos(SearchReposArgs),
}

#[derive(Debug, Args)]
pub struct SearchPeopleArgs {
    /// Search text.
    pub query: String,
    /// Maximum number of results.
    #[arg(long, default_value_t = 30, value_parser = parse_limit)]
    pub limit: usize,
    /// Emit a JSON array instead of human-readable rows.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SearchReposArgs {
    /// Search text.
    pub query: String,
    /// Restrict results to one user or organization namespace.
    #[arg(long)]
    pub owner: Option<String>,
    /// Restrict results to one repository type.
    #[arg(long = "type", value_enum)]
    pub repo_type: Option<RepoType>,
    /// Maximum number of results.
    #[arg(long, default_value_t = 30, value_parser = parse_limit)]
    pub limit: usize,
    /// Emit a JSON array instead of human-readable rows.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum RepoCommand {
    /// List repositories owned by a user or organization.
    List(RepoListArgs),
    /// List a repository tree, optionally filtered by globs.
    Tree(RepoTreeArgs),
    /// Write one repository file to standard output.
    Cat(RepoCatArgs),
}

#[derive(Debug, Args)]
pub struct RepoListArgs {
    /// User or organization namespace; defaults to the authenticated user.
    pub owner: Option<String>,
    /// Restrict results to one repository type.
    #[arg(long = "type", value_enum)]
    pub repo_type: Option<RepoType>,
    /// Maximum number of results.
    #[arg(long, default_value_t = 30, value_parser = parse_limit)]
    pub limit: usize,
    /// Emit a JSON array instead of human-readable rows.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RepoTreeArgs {
    /// Repository identifier in OWNER/NAME form.
    pub repo: RepoSpec,
    /// Exact paths or globs; prefix a quoted pattern with ! to subtract it.
    #[arg(value_name = "PATTERN")]
    pub patterns: Vec<String>,
    /// Repository type.
    #[arg(long = "type", value_enum, default_value_t)]
    pub repo_type: RepoType,
    /// Branch, tag, or commit.
    #[arg(long, default_value = "main")]
    pub revision: String,
    /// Emit a JSON array instead of human-readable rows.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct RepoCatArgs {
    /// Repository identifier in OWNER/NAME form.
    pub repo: RepoSpec,
    /// Exact repository-relative file path.
    pub path: String,
    /// Repository type.
    #[arg(long = "type", value_enum, default_value_t)]
    pub repo_type: RepoType,
    /// Branch, tag, or commit.
    #[arg(long, default_value = "main")]
    pub revision: String,
}

#[derive(Debug, Args)]
pub struct DownloadArgs {
    /// Repository identifier in OWNER/NAME form.
    pub repo: RepoSpec,
    /// Exact paths or globs; prefix a quoted pattern with ! to subtract it.
    #[arg(value_name = "PATTERN")]
    pub patterns: Vec<String>,
    /// Repository type.
    #[arg(long = "type", value_enum, default_value_t)]
    pub repo_type: RepoType,
    /// Branch, tag, or commit.
    #[arg(long, default_value = "main")]
    pub revision: String,
    /// Destination directory; defaults to the current directory.
    #[arg(long = "dir", value_name = "DIRECTORY")]
    pub directory: Option<PathBuf>,
    /// Replace existing regular files.
    #[arg(long)]
    pub force: bool,
    /// Maximum number of simultaneous HTTP transfers.
    #[arg(long, default_value_t = 3, value_parser = parse_jobs)]
    pub jobs: usize,
    /// Print selected paths without writing files.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Subcommand)]
pub enum AuthCommand {
    /// Validate and store a Hugging Face token.
    Login {
        /// Read the token from standard input instead of prompting.
        #[arg(long)]
        with_token: bool,
    },
    /// Show the authenticated account and token source.
    Status,
    /// Remove the token stored by xhf.
    Logout,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum RepoType {
    #[default]
    Model,
    Dataset,
    Space,
}

impl RepoType {
    pub fn singular(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Dataset => "dataset",
            Self::Space => "space",
        }
    }

    pub fn plural(self) -> &'static str {
        match self {
            Self::Model => "models",
            Self::Dataset => "datasets",
            Self::Space => "spaces",
        }
    }

    pub fn url_prefix(self) -> Option<&'static str> {
        match self {
            Self::Model => None,
            Self::Dataset => Some("datasets"),
            Self::Space => Some("spaces"),
        }
    }
}

impl Display for RepoType {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.singular())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepoSpec {
    owner: String,
    name: String,
}

impl RepoSpec {
    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl FromStr for RepoSpec {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((owner, name)) = value.split_once('/') else {
            return Err("repository must be written as OWNER/NAME".to_owned());
        };
        if owner.is_empty() || name.is_empty() || name.contains('/') {
            return Err("repository must contain exactly one non-empty OWNER/NAME pair".to_owned());
        }
        Ok(Self {
            owner: owner.to_owned(),
            name: name.to_owned(),
        })
    }
}

impl Display for RepoSpec {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.owner, self.name)
    }
}

fn parse_limit(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(0) => Err("limit must be greater than zero".to_owned()),
        Ok(limit) => Ok(limit),
        Err(error) => Err(format!("invalid limit: {error}")),
    }
}

fn parse_jobs(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(0) => Err("jobs must be greater than zero".to_owned()),
        Ok(jobs) => Ok(jobs),
        Err(error) => Err(format!("invalid jobs: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command, RepoCommand, RepoType};

    #[test]
    fn parses_negative_download_patterns() {
        let cli = Cli::try_parse_from([
            "xhf",
            "download",
            "owner/repo",
            "**/*.json",
            "!tests/**",
            "--type",
            "dataset",
        ])
        .unwrap();

        let Command::Download(args) = cli.command else {
            panic!("expected download command");
        };
        assert_eq!(args.patterns, ["**/*.json", "!tests/**"]);
        assert_eq!(args.repo_type, RepoType::Dataset);
        assert_eq!(args.jobs, 3);
    }

    #[test]
    fn parses_download_jobs_and_rejects_zero() {
        let cli = Cli::try_parse_from(["xhf", "download", "owner/repo", "--jobs", "7"]).unwrap();
        let Command::Download(args) = cli.command else {
            panic!("expected download command");
        };
        assert_eq!(args.jobs, 7);

        let result = Cli::try_parse_from(["xhf", "download", "owner/repo", "--jobs", "0"]);
        assert!(result.is_err());
    }

    #[test]
    fn parses_nested_repo_command() {
        let cli = Cli::try_parse_from(["xhf", "repo", "tree", "owner/repo", "models/**", "--json"])
            .unwrap();

        let Command::Repo {
            command: RepoCommand::Tree(args),
        } = cli.command
        else {
            panic!("expected repo tree command");
        };
        assert_eq!(args.repo.to_string(), "owner/repo");
        assert!(args.json);
    }

    #[test]
    fn rejects_zero_limit() {
        let result = Cli::try_parse_from(["xhf", "search", "users", "test", "--limit", "0"]);
        assert!(result.is_err());
    }
}
