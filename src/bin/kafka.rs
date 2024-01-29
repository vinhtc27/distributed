use distributed::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::StdoutLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    GossipSend {
        key: String,
        msg: usize,
    },
    GossipCommit {
        offsets: HashMap<String, usize>,
    },
}

enum InjectedPayload {
    GossipSend { key: String, msg: usize },
    GossipCommitOffsets { offsets: HashMap<String, usize> },
}

#[allow(dead_code)]
struct KafkaNode {
    node: String,
    id: usize,
    messages: Messages,
    others: Vec<String>,
    tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
}

// CommitedOffsetAndMessage
struct COAM {
    commited_offset: Option<usize>,
    msgs: Vec<usize>,
}

impl COAM {
    fn new() -> Self {
        Self {
            commited_offset: None,
            msgs: Vec::new(),
        }
    }
}

struct Messages {
    map: HashMap<String, COAM>,
}

impl Messages {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    fn add_msg(&mut self, key: String, msg: usize) -> usize {
        self.map
            .entry(key.clone())
            .or_insert_with(|| COAM::new())
            .msgs
            .push(msg);

        match self.map.get(&key) {
            Some(coam) => coam.msgs.len() - 1,
            None => 0,
        }
    }

    fn get_msgs(&self, offsets: &HashMap<String, usize>) -> HashMap<String, Vec<(usize, usize)>> {
        offsets
            .iter()
            .filter_map(|(key, offset)| {
                self.map.get(key).map(|coam| {
                    (
                        key.clone(),
                        coam.msgs[*offset..]
                            .iter()
                            .enumerate()
                            .map(|(i, m)| (offset + i, m.clone()))
                            .collect(),
                    )
                })
            })
            .collect()
    }

    fn insert_commited_offsets(&mut self, offsets: HashMap<String, usize>) {
        for (key, offset) in offsets {
            match self.map.get_mut(&key) {
                Some(coam) => coam.commited_offset = Some(offset),
                None => unreachable!(),
            }
        }
    }

    fn get_commited_offsets(&self, keys: &Vec<String>) -> HashMap<String, usize> {
        keys.iter()
            .filter_map(|key| {
                let commited_offset = self.map.get(key).map(|coam| coam.commited_offset);
                match commited_offset {
                    Some(Some(offset)) => Some((key.clone(), offset)),
                    _ => None,
                }
            })
            .collect()
    }
}

impl Node<(), Payload, InjectedPayload> for KafkaNode {
    fn from_init(
        _state: (),
        init: Init,
        tx: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            node: init.node_id.clone(),
            id: 1,
            messages: Messages::new(),
            others: init
                .node_ids
                .into_iter()
                .filter(|n| n != &init.node_id)
                .collect(),
            tx,
        })
    }

    fn step(
        &mut self,
        input: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::EOF => {}
            Event::Injected(payload) => match payload {
                InjectedPayload::GossipSend { key, msg } => {
                    for n in &self.others {
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::GossipSend {
                                    key: key.clone(),
                                    msg,
                                },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
                InjectedPayload::GossipCommitOffsets { offsets } => {
                    for n in &self.others {
                        Message {
                            src: self.node.clone(),
                            dst: n.clone(),
                            body: Body {
                                id: None,
                                in_reply_to: None,
                                payload: Payload::GossipCommit {
                                    offsets: offsets.clone(),
                                },
                            },
                        }
                        .send(&mut *output)
                        .with_context(|| format!("gossip to {}", n))?;
                    }
                }
            },
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.id));
                match reply.body.payload {
                    Payload::GossipSend { key, msg } => {
                        self.messages.add_msg(key, msg);
                    }
                    Payload::GossipCommit { offsets } => {
                        self.messages.insert_commited_offsets(offsets);
                    }
                    Payload::Send { key, msg } => {
                        let offset = self.messages.add_msg(key.clone(), msg);

                        reply.body.payload = Payload::SendOk { offset };
                        reply.send(&mut *output).context("reply to send")?;

                        self.tx
                            .send(Event::Injected(InjectedPayload::GossipSend { key, msg }))?;
                    }
                    Payload::Poll { offsets } => {
                        reply.body.payload = Payload::PollOk {
                            msgs: self.messages.get_msgs(&offsets),
                        };
                        reply.send(&mut *output).context("reply to poll")?;
                    }
                    Payload::CommitOffsets { offsets } => {
                        self.messages.insert_commited_offsets(offsets.clone());

                        reply.body.payload = Payload::CommitOffsetsOk;
                        reply
                            .send(&mut *output)
                            .context("reply to commit_offsets")?;

                        self.tx
                            .send(Event::Injected(InjectedPayload::GossipCommitOffsets {
                                offsets,
                            }))?;
                    }
                    Payload::ListCommittedOffsets { keys } => {
                        let offsets = self.messages.get_commited_offsets(&keys);

                        reply.body.payload = Payload::ListCommittedOffsetsOk { offsets };
                        reply
                            .send(&mut *output)
                            .context("reply to list_committed_offsets")?;
                    }
                    Payload::SendOk { .. }
                    | Payload::PollOk { .. }
                    | Payload::CommitOffsetsOk
                    | Payload::ListCommittedOffsetsOk { .. } => {}
                }
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaNode, _, _>(())
}
