use crate::{
    buffers::Acker,
    event::Event,
    sinks::file::{EmbeddedFileSink, FileSink},
    sinks::util::{
        encoding::{self, BasicEncoding},
        SinkExt,
    },
    template::Template,
    topology::config::DataType,
};

use futures::{future, Async, AsyncSink, Sink, StartSend};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Deserialize, Serialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct PartitionedFileSinkConfig {
    pub path_template: String,
    #[serde(default = "default_close_timeout_secs")]
    pub close_timeout_secs: u64,
    pub encoding: Option<BasicEncoding>,
}

fn default_close_timeout_secs() -> u64 {
    60
}

impl PartitionedFileSinkConfig {
    pub fn new(path_template: String) -> Self {
        Self {
            path_template,
            close_timeout_secs: default_close_timeout_secs(),
            encoding: None,
        }
    }
}

#[typetag::serde(name = "partitioned_file")]
impl crate::topology::config::SinkConfig for PartitionedFileSinkConfig {
    fn build(&self, acker: Acker) -> Result<(super::RouterSink, super::Healthcheck), String> {
        let sink = PartitionedFileSink::new(
            Template::from(&self.path_template[..]),
            self.encoding.clone(),
        )
        .stream_ack(acker);

        Ok((Box::new(sink), Box::new(future::ok(()))))
    }

    fn input_type(&self) -> DataType {
        DataType::Log
    }
}

pub struct PartitionedFileSink {
    path_template: Template,
    encoding: Option<BasicEncoding>,
    partitions: HashMap<PathBuf, EmbeddedFileSink>,
    //todo: implement closing of files basing on timeout
}

impl PartitionedFileSink {
    pub fn new(path_template: Template, encoding: Option<BasicEncoding>) -> Self {
        PartitionedFileSink {
            path_template,
            encoding,
            partitions: HashMap::new(),
        }
    }
}

impl Sink for PartitionedFileSink {
    type SinkItem = Event;
    type SinkError = ();

    fn start_send(&mut self, event: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self.path_template.render(&event) {
            Ok(bytes) => {
                let path = PathBuf::from(String::from_utf8_lossy(&bytes).as_ref());

                let partition = self
                    .partitions
                    .entry(path.clone())
                    .or_insert(FileSink::new_with_encoding(&path, self.encoding.clone()));

                partition.start_send(event)
            }

            Err(missing_keys) => {
                warn!(
                    message = "Keys do not exist on the event. Dropping event.",
                    keys = ?missing_keys
                );
                Ok(AsyncSink::Ready)
            }
        }
    }

    fn poll_complete(&mut self) -> Result<Async<()>, Self::SinkError> {
        self.partitions.iter_mut().for_each(|(path, partition)| {
            match partition.poll_complete() {
                Ok(_) => {}

                Err(()) => {
                    error!("Error in downstream FileSink with path {:?}", path);
                    //todo: close file sink
                }
            }
        });

        Ok(Async::Ready(()))
    }
}
