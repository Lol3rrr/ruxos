use maelstrom_api::{Message, MessageBody};

#[test]
fn serialize_deserialize() {
    let mut tmp = Vec::new();

    let mut sender = maelstrom_api::Sender::new(&mut tmp);

    let send = Message::new("src", "dest", MessageBody::new(None, None, ()));
    sender.send(send.clone());

    let mut receiver = maelstrom_api::Receiver::new(tmp.as_slice());

    let resp = receiver.recv().unwrap();

    assert_eq!(send, resp);
}
