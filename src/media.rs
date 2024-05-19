use std::{
    collections::HashMap,
    ffi::OsStr,
    io::ErrorKind,
    ops::Deref,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, Context as _};
use log::{debug, warn};
use symphonia::core::{
    codecs::{DecoderOptions, CODEC_TYPE_NULL},
    errors::Error,
    formats::FormatOptions,
    io::MediaSourceStream,
    meta::{MetadataOptions, StandardTagKey, Value},
    probe::Hint,
};
use walkdir::DirEntry;

use crate::types;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TagKey {
    Standard(StandardTagKey),
    Custom(String),
}

#[derive(Debug, Clone)]
pub struct TagMap(HashMap<TagKey, Value>);
impl TagMap {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl Default for TagMap {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for TagMap {
    type Target = HashMap<TagKey, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct MusicFile {
    path: PathBuf,
    tags: TagMap,
}

const MEDIA_EXTENSIONS: [&str; 8] = ["mp3", "m4a", "aac", "ogg", "opus", "flac", "alac", "wav"];

impl MusicFile {
    pub fn parse_with_symphonia(file: types::File<impl AsRef<Path>>) -> anyhow::Result<Self> {
        let file = file.as_ref();

        debug!("{file:?}: begin parsing");

        // Open the media source.
        let src = std::fs::File::open(file)?;

        // Create the media source stream.
        let mss = MediaSourceStream::new(Box::new(src), Default::default());

        // Create a probe hint using the file's extension. [Optional]
        let mut hint = Hint::new();
        if let Some(Some(extension)) = file.extension().map(OsStr::to_str) {
            hint.with_extension(extension);
        }

        // Use the default options for metadata and format readers.
        let meta_opts: MetadataOptions = Default::default();
        let fmt_opts: FormatOptions = FormatOptions {
            enable_gapless: true,
            ..Default::default()
        };

        // Probe the media source.
        let mut probed = symphonia::default::get_probe()
            .format(&hint, mss, &fmt_opts, &meta_opts)
            .with_context(|| format!("{file:?}: unsupported format"))?;

        // Get the instantiated format reader.
        let mut format = probed.format;
        let mut metadata_log = probed
            .metadata
            .get()
            .ok_or_else(|| anyhow!("{file:?}: No metadata found"))?;

        // Find the first audio track with a known (decodeable) codec.
        let track = format
            .tracks()
            .iter()
            .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
            .ok_or_else(|| anyhow!("{file:?}: no supported audio tracks"))?;

        // Use the default options for the decoder.
        let dec_opts: DecoderOptions = Default::default();

        // Create a decoder for the track.
        let mut decoder = symphonia::default::get_codecs()
            .make(&track.codec_params, &dec_opts)
            .with_context(|| format!("{file:?}: unsupported codec"))?;

        // Store the track identifier, it will be used to filter packets.
        let track_id = track.id;
        let mut tags = TagMap::new();

        // TODO put this in a loop, while decoding
        // TODO split this
        // The decode loop.
        loop {
            // Get the next packet from the media format.
            let packet = match format.next_packet() {
                Ok(packet) => packet,
                Err(Error::IoError(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                    debug!("{file:?}: found EOF");
                    break;
                }
                Err(Error::ResetRequired) => {
                    // The track list has been changed. Re-examine it and create a new set of decoders,
                    // then restart the decode loop. This is an advanced feature and it is not
                    // unreasonable to consider this "the end." As of v0.5.0, the only usage of this is
                    // for chained OGG physical streams.
                    bail!("{file:?}: Track list has been changed. This is unsupported and _very_ rare. (Error::ResetRequired)");
                }
                Err(err) => {
                    // A unrecoverable error occurred, halt decoding.
                    bail!("{file:?}: {}", err);
                }
            };

            // Consume any new metadata that has been read since the last packet.
            while let Some(metadata) = metadata_log.pop() {
                // update HashMap
                metadata.tags().iter().for_each(|tag| {
                    let key = if let Some(key) = tag.std_key {
                        TagKey::Standard(key)
                    } else {
                        TagKey::Custom(tag.key.clone())
                    };
                    let value = tag.value.clone();
                    let old_value = tags.0.insert(key.clone(), value.clone());
                    if let Some(old_value) = old_value {
                        debug!("{file:?}: tag override on {key:?} ({old_value} => {old_value})");
                    }
                })
                // Consume the new metadata at the head of the metadata queue.
            }

            // If the packet does not belong to the selected track, skip over it.
            if packet.track_id() != track_id {
                continue;
            }

            // Decode the packet into audio samples.
            match decoder.decode(&packet) {
                Ok(_decoded) => {
                    // Consume the decoded audio samples (see below).
                    ()
                }
                Err(Error::IoError(err)) => match err.kind() {
                    // The packet failed to decode due to an IO error, skip the packet.
                    _ => warn!("{file:?}: IO error at {} ({err})", packet.ts),
                },
                Err(Error::DecodeError(err)) => {
                    // The packet failed to decode due to invalid data, skip the packet.
                    warn!("{file:?}: decoding error at {} ({err})", packet.ts);
                    continue;
                }
                Err(err) => {
                    // An unrecoverable error occurred, halt decoding.
                    bail!("{}: unrecoverable decoding error ({err})", err);
                }
            }
        }

        Ok(Self {
            tags,
            path: file.to_owned(),
        })
    }
}

pub fn is_media_file_by_name(f: &DirEntry) -> bool {
    f.file_type().is_file()
        && MEDIA_EXTENSIONS.contains(&f.path().extension().and_then(OsStr::to_str).unwrap_or(""))
}
