use std::io::{BufRead, BufReader, BufWriter, Read, Stdin, Stdout, Write};

pub mod workflow;
pub use workflow::{Message, MessageBody};

pub struct Receiver<R> {
    reader: BufReader<R>,
}

pub struct Sender<W>
where
    W: Write,
{
    writer: BufWriter<W>,
}

impl<R> Receiver<R> {}

impl<W> Sender<W>
where
    W: Write,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }
}

impl<W> Sender<W>
where
    W: Write,
{
    pub fn send<B>(&mut self, msg: Message<B>)
    where
        B: serde::Serialize,
    {
        serde_json::to_writer(&mut self.writer, &msg).unwrap();
        writeln!(self.writer).unwrap();
        self.writer.flush().unwrap();
    }
}

impl<R> Receiver<R>
where
    R: Read,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    pub fn recv<B>(&mut self) -> Result<Message<B>, ()>
    where
        B: serde::de::DeserializeOwned,
    {
        let mut tmp = String::new();
        self.reader.read_line(&mut tmp).map_err(|e| {
            eprintln!("Error: {:?}", e);
            ()
        })?;

        serde_json::from_str(&tmp).map_err(|e| {
            eprintln!("Error: {:?}", e);
            ()
        })
    }
}

pub fn io_recv_send() -> (Sender<Stdout>, Receiver<Stdin>) {
    let stdout = std::io::stdout();
    let stdin = std::io::stdin();

    (Sender::new(stdout), Receiver::new(stdin))
}

pub fn init<R, W, F, T>(
    receiver: &mut Receiver<R>,
    sender: &mut Sender<W>,
    init_fn: F,
) -> Result<T, ()>
where
    R: Read,
    W: Write,
    F: FnOnce(&str, &[String]) -> T,
{
    let msg = receiver
        .recv::<workflow::general::Request>()
        .map_err(|_e| ())?;

    let (id, nodes) = match msg.body().content() {
        workflow::general::Request::Init { node_id, node_ids } => (node_id, node_ids),
    };

    let res = init_fn(id, nodes);

    sender.send(msg.reply(msg.body().reply(0, workflow::general::Response::InitOk)));

    Ok(res)
}
