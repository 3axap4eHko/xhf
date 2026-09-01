mod app;
mod cli;
mod config;
mod download;
mod error;
mod hub;
mod patterns;
mod text;

use std::io::{self, IsTerminal};
use std::process::ExitCode;

use clap::Parser;

use crate::cli::Cli;
use crate::config::RuntimeContext;

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = if error.use_stderr() { 2 } else { 0 };
            if let Err(print_error) = error.print() {
                eprintln!("xhf: failed to print command-line help: {print_error}");
                return ExitCode::FAILURE;
            }
            return ExitCode::from(exit_code);
        }
    };

    let runtime = match RuntimeContext::capture() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("xhf: {error}");
            return ExitCode::FAILURE;
        }
    };

    let stdout = io::stdout();
    let mut output = stdout.lock();
    let stderr = io::stderr();
    let interactive_progress = stderr.is_terminal();
    let result = {
        let mut progress = stderr.lock();
        app::run(
            cli,
            &runtime,
            &mut output,
            &mut progress,
            interactive_progress,
        )
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.is_broken_pipe() => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("xhf: {error}");
            ExitCode::FAILURE
        }
    }
}
