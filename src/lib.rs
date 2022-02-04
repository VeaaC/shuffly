//! Increases compressability of data with fixed-sized records.
//!
//! `Shuffly` detects fixed-sized data patterns by trying out different
//! pattern sized between (by default 1 to 64 bytes). For each pattern it reorderd bytes
//! such that byte X of each record is group together, and stores deltas of
//! these bytes instead of the original data.
//!
//! The resulting data stream is much more compressible for pattern based
//! compression algorithms like deflate /gz, zip, etc), zstd, or lzma.
//!
//! Shuffly is available both as a command line app (e.g. `cargo install shuffly`),
//! and a library.
//!
//! Library usage
//! ```no_run
//! # let input = "foo";
//! # let output = "bar";
//! # let encode = true;
//! # use std::fs::File;
//! let input: shuffly::Input = Box::new(File::open(input).unwrap());
//! let output: shuffly::Output = Box::new(File::create(output).unwrap());
//! let options = shuffly::Options::new();
//! if encode {
//!     shuffly::encode(0, input, output, &shuffly::Options::new()).unwrap();
//! } else {
//!     shuffly::decode(0, input, output).unwrap();
//! }
//! ```

use std::io::prelude::*;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::HashMap;

use crossbeam::channel;

use lz4::EncoderBuilder;

const TAG: [u8; 7] = *b"SHUFFLY";
const HEADER_SIZE: usize = TAG.len() + 0_u64.to_be_bytes().len();
const STRIDE_SIZE: usize = 0_u16.to_be_bytes().len();

/// Lists (non-exhaustivly) all possible error scenarios
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to create threads")]
    ThreadPool,
    #[error("Failed reading input: {0}")]
    Input(std::io::Error),
    #[error("Failed writing output: {0}")]
    Output(std::io::Error),
    #[error("Encoding is not in SHUFFLY format")]
    Encoding,
}

fn parallel_process<Iter, Item, Producer, Data, Consumer>(
    threads: usize,
    iter: Iter,
    produce: Producer,
    mut consume: Consumer,
) -> Result<(), Error>
where
    Iter: Iterator<Item = Item> + Send,
    Item: Send,
    Producer: Fn(Item) -> Data + Sync,
    Data: Send,
    Error: Send,
    Consumer: FnMut(Data) -> Result<(), Error> + Send,
{
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads + 2) // we need 2 extra threads for blocking I/O
        .build()
        .map_err(|_| Error::ThreadPool)?;

    let num_tokens = 2 * threads;

    let (token_sender, token_reciver) = channel::bounded(num_tokens);
    let (iter_sender, iter_receiver) = channel::bounded(num_tokens);
    let (data_sender, data_receiver) = channel::bounded(num_tokens);

    pool.scope(|s| {
        s.spawn(|_| {
            for x in iter.enumerate() {
                if token_reciver.recv().is_err() {
                    break;
                }
                if iter_sender.send(x).is_err() {
                    break;
                }
            }
            std::mem::drop(iter_sender);
        });

        for _ in 0..threads {
            let data_sender = data_sender.clone();
            s.spawn(|_| {
                let data_sender = data_sender;
                while let Ok((i, item)) = iter_receiver.recv() {
                    let data = produce(item);
                    if data_sender.send((i, data)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(data_sender); // drop to make sure iteration will finish once all senders are out of scope

        // we need to move these into the scope so they are dropped on failure
        let token_sender = token_sender;
        let data_receiver = data_receiver;
        let mut pending = BTreeMap::new();
        let mut next_idx = 0;
        for _ in 0..num_tokens {
            if token_sender.send(()).is_err() {
                return Err(Error::ThreadPool);
            }
        }
        for result in data_receiver {
            pending.insert(Reverse(result.0), result.1);
            while let Some(data) = pending.remove(&Reverse(next_idx)) {
                if token_sender.send(()).is_err() {
                    return Err(Error::ThreadPool);
                }

                next_idx += 1;
                consume(data)?;
            }
        }
        Ok(())
    })
}

pub type Input = Box<dyn std::io::Read + Send + Sync>;
pub type Output = Box<dyn std::io::Write + Send + Sync>;

/// Read data until we have read a full block or reached the end of the input
fn read_block(
    mut input: Input,
    block_size: usize,
) -> impl FnMut() -> Option<Result<Vec<u8>, Error>> {
    let mut eof = false;
    move || {
        if eof {
            return None;
        }
        let mut buf = vec![0_u8; block_size];
        // read one full block if possible
        let mut block_size = 0;
        while !buf[block_size..].is_empty() {
            let num_read = match input.read(&mut buf[block_size..]) {
                Err(e) => return Some(Err(Error::Input(e))),
                Ok(x) => x,
            };
            if num_read == 0 {
                eof = true;
                break;
            }
            block_size += num_read;
        }

        buf.resize(block_size, 0);
        if buf.is_empty() {
            None
        } else {
            Some(Ok(buf))
        }
    }
}

/// Compute the amount of information present in the distribution of the 8-bit symbols
fn compute_information(symbol_counts: [usize; 256]) -> usize {
    let count: usize = symbol_counts.iter().sum();
    symbol_counts
        .into_iter()
        .map(|x| {
            if x == 0 {
                0
            } else {
                (x as f64 * (count as f64 / x as f64).log2()) as usize
            }
        })
        .sum()
}

/// Finds the most possible stride by trying all of them on a subset of the data
fn find_best_stride(buf: &[u8], strides: &[u16]) -> usize {
    if strides.is_empty() {
        return 0;
    }
    let max_stride = strides.iter().copied().max().unwrap_or(0) as usize;
    if buf.len() <= max_stride {
        return 0;
    }
    let (mut best_stride, mut best_score) = (0, {
        let mut symbol_counts = [0; 256];
        for chunk in buf[max_stride..].chunks(64).step_by(4) {
            for x in chunk {
                symbol_counts[*x as usize] += 1;
            }
        }
        compute_information(symbol_counts)
    });
    for i in 1..=max_stride {
        let mut symbol_counts = [0; 256];
        for (a_chunk, b_chunk) in buf[max_stride..]
            .chunks(64)
            .step_by(4)
            .zip(buf[max_stride - i..].chunks(64).step_by(4))
        {
            for (a, b) in a_chunk.iter().zip(b_chunk) {
                symbol_counts[a.wrapping_sub(*b) as usize] += 1;
            }
        }

        let new_score = compute_information(symbol_counts);
        if new_score < best_score {
            best_score = new_score;
            best_stride = i;
        }
    }
    best_stride
}

fn shuffle(buf: &[u8], stride: usize) -> Vec<u8> {
    let mut out_buf = Vec::with_capacity(buf.len() + STRIDE_SIZE);
    // write block header
    out_buf.extend((stride as u16).to_be_bytes() as [u8; STRIDE_SIZE]);

    if stride == 0 {
        out_buf.extend(buf);
    } else {
        // Transform payload:
        // * reorder bytes so that bytes X of each stride comes before bytes X+1 of any other stride
        // * compute deltas between bytes
        for i in 0..stride {
            let mut previous = 0;
            for x in buf.iter().skip(i).step_by(stride) {
                out_buf.push(x.wrapping_sub(previous));
                previous = *x;
            }
        }
    }
    out_buf
}

fn deshuffle(buf: &[u8]) -> Result<Vec<u8>, Error> {
    let stride = match buf.get(0..STRIDE_SIZE) {
        None => return Err(Error::Encoding),
        Some(x) => u16::from_be_bytes(x.try_into().unwrap()) as usize,
    };

    let buf = &buf[STRIDE_SIZE..];

    if stride == 0 {
        return Ok(buf.into());
    }
    let mut out_buf = vec![0_u8; buf.len()];
    let mut iter = buf.iter();
    for i in 0..stride {
        let mut previous: u8 = 0;
        for pos in out_buf.iter_mut().skip(i).step_by(stride) {
            // we cannot fail here, since out_buf was sized properly
            let x = iter.next().unwrap();
            previous = previous.wrapping_add(*x);
            *pos = previous;
        }
    }

    Ok(out_buf)
}

fn compressability(buf: &[u8]) -> Option<usize> {
    let output = Vec::new();
    let mut encoder = EncoderBuilder::new()
        .level(4)
        .build(output)
        .expect("Invalid compression level");
    if encoder.write_all(buf).is_err() {
        return None;
    }
    match encoder.finish() {
        (output, Ok(_)) => Some(output.len()),
        _ => None,
    }
}

/// Statistics about the encoding process
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Stats {
    /// Which strides were used how often
    pub strides: Vec<(usize, u64)>,
}

/// Options passed to the encoder
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Options {
    pub block_size: usize,
    pub strides: Vec<u16>,
}

impl Options {
    pub fn new() -> Self {
        Self {
            block_size: 1024 * 1024,
            strides: (0..=64).collect(),
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
}

/// Encodes data by searching for fixed-sized correctlation in the data and
/// shuffling data around accordingly.
/// At least 3 threads will be used, one for input, one for output, and one for encoding
pub fn encode(
    threads: usize,
    input: Input,
    mut output: Output,
    options: &Options,
) -> Result<Stats, Error> {
    // write header
    output.write_all(b"SHUFFLY").map_err(Error::Output)?;
    output
        .write_all(&(options.block_size as u64).to_be_bytes())
        .map_err(Error::Output)?;
    let mut strides = HashMap::new();
    parallel_process(
        threads,
        std::iter::from_fn(read_block(input, options.block_size)),
        |buf: Result<Vec<u8>, Error>| {
            let buf = buf?;
            let stride = find_best_stride(&buf, &options.strides);
            // lets do a quick sanity check if we are not making things worse
            let prefix = &buf[..buf.len().min(1024 * 4)];
            let shuffled_prefix = shuffle(prefix, stride);
            if let (Some(original), Some(shuffled)) =
                (compressability(prefix), compressability(&shuffled_prefix))
            {
                if original < shuffled {
                    // the shuffled version is less compressible than the original one
                    return Ok((0, shuffle(&buf, 0)));
                }
            }
            Ok((stride, shuffle(&buf, stride)))
        },
        |data| {
            let (stride, buf) = data?;
            *strides.entry(stride).or_insert(0) += 1;
            output.write_all(&buf).map_err(Error::Output)
        },
    )?;
    let mut strides: Vec<_> = strides.into_iter().collect();
    strides.sort_unstable();
    Ok(Stats { strides })
}

/// Decoded data that has previously been encoded.
/// At least 3 threads will be used, one for input, one for output, and one for encoding
pub fn decode(threads: usize, mut input: Input, mut output: Output) -> Result<(), Error> {
    // read header
    let mut header: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
    input.read_exact(&mut header).map_err(Error::Input)?;
    if header[0..TAG.len()] != TAG {
        return Err(Error::Encoding);
    }

    // unwrapping here since we cannot fail
    let block_size = u64::from_be_bytes(header[TAG.len()..].try_into().unwrap());
    let block_size: usize = match block_size.try_into() {
        Err(_) => return Err(Error::Encoding),
        Ok(x) => x,
    };
    parallel_process(
        threads,
        std::iter::from_fn(read_block(input, block_size + STRIDE_SIZE)),
        |buf: Result<Vec<u8>, Error>| {
            let buf = buf?;
            deshuffle(&buf)
        },
        |buf| output.write_all(&buf?).map_err(Error::Output),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct CaptureWrite {
        data: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for CaptureWrite {
        fn write(&mut self, value: &[u8]) -> std::result::Result<usize, std::io::Error> {
            self.data.lock().unwrap().write(value)
        }
        fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
            Ok(())
        }
    }

    #[test]
    fn end2end() {
        for size in [0, 5, 7, 13, 64, 128, 1024, 1024 * 1024] {
            let input: Vec<_> = (0_u64..size)
                .map(|i| match i % 5 {
                    0 => i,
                    1 => i * 2 + 399,
                    2 => i * 3 + 300,
                    3 => i * i,
                    _ => i + i * i,
                } as u8)
                .collect();
            println!("Input: {:?}", &input);

            let shuffled = CaptureWrite::default();
            encode(
                2,
                Box::new(std::io::Cursor::new(input.clone())),
                Box::new(shuffled.clone()),
                &Options {
                    block_size: 19,
                    ..Options::new()
                },
            )
            .expect("Failed to encode");
            let shuffled = Arc::try_unwrap(shuffled.data)
                .unwrap()
                .into_inner()
                .unwrap();
            println!("Shuffled: {:?}", &shuffled);

            let deshuffled = CaptureWrite::default();
            decode(
                2,
                Box::new(std::io::Cursor::new(shuffled.clone())),
                Box::new(deshuffled.clone()),
            )
            .expect("Failed to decode");
            let deshuffled = Arc::try_unwrap(deshuffled.data)
                .unwrap()
                .into_inner()
                .unwrap();
            println!("Deshuffled: {:?}", &deshuffled);

            assert!(deshuffled == input);
        }
    }

    #[test]
    fn shuffle_deshuffle() {
        for size in [0, 5, 7, 13, 64, 128, 1024] {
            let input: Vec<_> = (0_u64..size)
                .map(|i| match i % 5 {
                    0 => i,
                    1 => i * 2 + 399,
                    2 => i * 3 + 300,
                    3 => i * i,
                    _ => i + i * i,
                } as u8)
                .collect();

            let shuffled = shuffle(&input, 5);
            let deshuffled = deshuffle(&shuffled).unwrap();

            assert_eq!(
                deshuffled, input,
                "\n{:?} -> {:?} -> {:?}",
                input, shuffled, deshuffled
            );
        }
    }
}
