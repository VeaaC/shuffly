use std::io::prelude::*;

use std::cmp::Reverse;
use std::collections::BTreeMap;

use crossbeam::channel;

const TAG: [u8; 7] = *b"SHUFFLY";
const HEADER_SIZE: usize = TAG.len() + 0_u64.to_be_bytes().len();
const STRIDE_SIZE: usize = 0_u16.to_be_bytes().len();
const MAX_STRIDE: usize = 64;

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
    symbol_counts
        .into_iter()
        .map(|x| {
            if x == 0 {
                0
            } else {
                x * (0_usize.leading_zeros() - (usize::MAX / x).leading_zeros()) as usize
            }
        })
        .sum()
}

/// Finds the most possible stride by trying all of them on a subset of the data
fn find_best_stride(buf: &[u8]) -> usize {
    if buf.len() <= MAX_STRIDE {
        return 0;
    }
    let (mut stride, mut score) = (0, {
        let mut symbol_counts = [0; 256];
        for chunk in buf[MAX_STRIDE..].chunks(64).step_by(4) {
            for x in chunk {
                symbol_counts[*x as usize] += 1;
            }
        }
        compute_information(symbol_counts)
    });
    for i in 1..=MAX_STRIDE {
        let mut symbol_counts = [0; 256];
        for (a_chunk, b_chunk) in buf[MAX_STRIDE..]
            .chunks(64)
            .step_by(4)
            .zip(buf[MAX_STRIDE - i..].chunks(64).step_by(4))
        {
            for (a, b) in a_chunk.iter().zip(b_chunk) {
                symbol_counts[a.wrapping_sub(*b) as usize] += 1;
            }
        }

        let new_score = compute_information(symbol_counts);
        if new_score < score {
            stride = i;
            if new_score * 3 < score * 2 {
                // the first noticable improvement is very likely the best global one
                // break;
            }
            score = new_score;
        }
    }
    stride
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

/// Encodes data by searching for fixed-sized correctlation in the data and
/// shuffling data around accordingly.
/// At least 3 threads will be used, one for input, one for output, and one for encoding
pub fn encode(
    threads: usize,
    input: Input,
    mut output: Output,
    block_size: usize,
) -> Result<(), Error> {
    // write header
    output.write_all(b"SHUFFLY").map_err(Error::Output)?;
    output
        .write_all(&(block_size as u64).to_be_bytes())
        .map_err(Error::Output)?;
    parallel_process(
        threads,
        std::iter::from_fn(read_block(input, block_size)),
        |buf: Result<Vec<u8>, Error>| {
            let buf = buf?;
            let stride = find_best_stride(&buf);
            Ok(shuffle(&buf, stride))
        },
        |buf| output.write_all(&buf?).map_err(Error::Output),
    )
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
                19,
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