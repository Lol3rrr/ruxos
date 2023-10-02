pub mod general {
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum Request {
        Init {
            node_id: String,
            node_ids: Vec<String>,
        },
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum Response {
        InitOk,
    }
}

pub mod linear_kv {
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum Request<Custom> {
        Read { key: usize },
        Write { key: usize, value: usize },
        Cas { key: usize, from: usize, to: usize },
        Custom { content: Custom },
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum Response {
        ReadOk { value: usize },
        WriteOk,
        CasOk,
        Error { code: usize, text: String },
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Message<C> {
    src: String,
    dest: String,
    body: MessageBody<C>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MessageBody<C> {
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    content: C,
}

impl<C> Message<C> {
    pub fn new<S, D>(src: S, dest: D, body: MessageBody<C>) -> Self
    where
        S: Into<String>,
        D: Into<String>,
    {
        Self {
            src: src.into(),
            dest: dest.into(),
            body,
        }
    }

    pub fn reply<C2>(&self, body: MessageBody<C2>) -> Message<C2> {
        Message {
            src: self.dest.clone(),
            dest: self.src.clone(),
            body,
        }
    }

    pub fn body(&self) -> &MessageBody<C> {
        &self.body
    }

    pub fn src(&self) -> &str {
        &self.src
    }
}

impl<C> MessageBody<C> {
    pub fn new(msg_id: Option<u64>, in_reply_to: Option<u64>, content: C) -> Self {
        Self {
            msg_id,
            in_reply_to,
            content,
        }
    }

    pub fn reply<C2>(&self, msg_id: u64, content: C2) -> MessageBody<C2> {
        MessageBody {
            msg_id: Some(msg_id),
            in_reply_to: self.msg_id,
            content,
        }
    }

    pub fn id(&self) -> Option<u64> {
        self.msg_id
    }

    pub fn replied_to(&self) -> Option<u64> {
        self.in_reply_to
    }

    pub fn content(&self) -> &C {
        &self.content
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_read_op() {
        let result = serde_json::to_string(&Message::new(
            "from",
            "to",
            MessageBody::new(None, None, linear_kv::Request::<()>::Read { key: 0 }),
        ))
        .unwrap();

        assert_eq!(
            "{\"src\":\"from\",\"dest\":\"to\",\"body\":{\"type\":\"read\",\"key\":0}}",
            result
        );
    }
}
