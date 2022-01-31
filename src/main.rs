use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;
use structopt::StructOpt;

fn parse_size(x: &str) -> anyhow::Result<usize> {
    let x = x.to_ascii_lowercase();
    if let Some(value) = x.strip_suffix("gb") {
        return Ok(usize::from_str(value)? * 1024 * 1024 * 1024);
    }
    if let Some(value) = x.strip_suffix("mb") {
        return Ok(usize::from_str(value)? * 1024 * 1024);
    }
    if let Some(value) = x.strip_suffix("kb") {
        return Ok(usize::from_str(value)? * 1024);
    }
    anyhow::bail!("Cannot parse size: '{}'", x)
}

#[derive(StructOpt, Debug)]
#[structopt(name = "deshufi")]
struct Args {
    /// input file name
    #[structopt(long, short = "i")]
    input: Option<PathBuf>,

    /// output file name
    #[structopt(long, short = "o")]
    output: Option<PathBuf>,

    /// block size used for data shuffling and compression
    #[structopt(long, default_value = "1MB", parse(try_from_str=parse_size))]
    block_size: usize,

    /// encodes input into SHUFFLY format
    #[structopt(long, short = "e")]
    encode: bool,

    /// decodes input from SHUFFLY format
    #[structopt(long, short = "d")]
    decode: bool,

    /// number of threads to use, defaults to number of logical cores
    #[structopt(long, short = "t")]
    threads: Option<usize>,
}

fn main() {
    let args = Args::from_args();

    let mut threads = args.threads.unwrap_or(0);
    if threads == 0 {
        threads = num_cpus::get();
    }

    let input: shuffly::Input = if let Some(file) = args.input {
        Box::new(match File::open(file) {
            Err(e) => {
                eprintln!("Failed to open input file: {}", e);
                std::process::exit(1);
            }
            Ok(x) => x,
        })
    } else {
        Box::new(std::io::stdin())
    };

    let output: shuffly::Output = if let Some(file) = args.output {
        Box::new(match File::create(file) {
            Err(e) => {
                eprintln!("Failed to open output file: {}", e);
                std::process::exit(1);
            }
            Ok(x) => x,
        })
    } else {
        Box::new(std::io::stdout())
    };

    let result = if args.encode {
        shuffly::encode(threads, input, output, args.block_size)
    } else if args.decode {
        shuffly::decode(threads, input, output)
    } else {
        eprintln!("Specified neither encode nor decode");
        std::process::exit(1);
    };

    if let Err(e) = result {
        eprintln!("Failed: {}", e);
        std::process::exit(1);
    }
}
