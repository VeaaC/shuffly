use std::fs::File;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
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

fn parse_strides(x: &str) -> anyhow::Result<Vec<u16>> {
    let mut result = Vec::new();
    if let Some((first, second)) = x.split_once("..=") {
        let range: std::ops::RangeInclusive<u16> = first.parse()?..=second.parse()?;
        result.extend(range);
    } else if let Some((first, second)) = x.split_once("..") {
        let range: std::ops::Range<u16> = first.parse()?..second.parse()?;
        result.extend(range);
    } else {
        result.push(x.parse()?)
    }
    Ok(result)
}

#[derive(StructOpt, Debug)]
#[structopt(name = "shuffly")]
struct Args {
    /// input file name
    #[structopt(long, short = "i")]
    input: Option<PathBuf>,

    /// output file name
    #[structopt(long, short = "o")]
    output: Option<PathBuf>,

    /// block size used for data shuffling and compression
    #[structopt(long, default_value = "4MB", parse(try_from_str=parse_size))]
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

    /// Print verbose information, statistics, etc
    #[structopt(long, short = "v")]
    verbose: bool,

    // specifies which strides should be used to try to detect fixed-sized patterns
    #[structopt(long, default_value = "0..65", parse(try_from_str=parse_strides), use_delimiter = true)]
    strides: Vec<Vec<u16>>,
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

    let now = Instant::now();

    let result = if args.encode {
        let mut options = shuffly::Options::new();
        options.block_size = args.block_size;
        options.strides = args.strides.iter().flatten().copied().collect();
        shuffly::encode(threads, input, output, &options).map(|stats| {
            if !args.verbose {
                return;
            }
            eprintln!("Stride statistics:");
            for (stride, count) in stats.strides {
                eprintln!("    Stride {}, blocks {}", stride, count);
            }
        })
    } else if args.decode {
        shuffly::decode(threads, input, output)
    } else {
        eprintln!("Specified neither encode nor decode");
        std::process::exit(1);
    };

    if args.verbose {
        eprintln!("Took: {}s", now.elapsed().as_secs())
    }

    if let Err(e) = result {
        eprintln!("Failed: {}", e);
        std::process::exit(1);
    }
}
