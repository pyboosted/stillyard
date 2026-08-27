use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "stillyard", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print generated public schemas.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SchemaCommand {
    /// Print the versioned JobSpec/BatchSpec schema.
    Spec,
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Schema {
            command: SchemaCommand::Spec,
        } => stillyard::schema_json().map(|schema| print!("{schema}")),
    };

    if let Err(error) = result {
        eprintln!("stillyard: {error}");
        std::process::exit(70);
    }
}
