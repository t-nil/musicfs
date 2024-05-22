use std::{
    collections::HashMap,
    ffi::OsStr,
    io::ErrorKind,
    ops::Deref,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, ensure, Context as _, Result};
use derive_getters::Getters;
use log::{debug, warn};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use symphonia::core::{
    codecs::{DecoderOptions, CODEC_TYPE_NULL},
    errors::Error,
    formats::FormatOptions,
    io::MediaSourceStream,
    meta::{MetadataOptions, StandardTagKey, Value},
    probe::Hint,
};
use walkdir::DirEntry;

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

/// MusicFiles are Eq if paths are Eq
#[derive(Debug, Clone, Getters)]
pub struct MusicFile {
    path: PathBuf,
    tags: TagMap,
}

impl PartialEq for MusicFile {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for MusicFile {}

const MEDIA_EXTENSIONS: [&str; 8] = ["mp3", "m4a", "aac", "ogg", "opus", "flac", "alac", "wav"];

impl MusicFile {
    pub fn instantiate_template(&self, template: &str) -> Result<PathBuf> {
        use StandardTagKey::*;
        use TagKey::*;
        static RGX_TAG: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"%(\w+)%").expect("static regex failed to compile"));

        let tag = |key: &TagKey| {
            let tags = &self.tags.0;
            tags.get(key).map(Value::to_string).unwrap_or_else(|| {
                warn!("${self:?}: {key:?} not found during formatting");
                "$not_found$".to_string()
            })
        };

        let path = RGX_TAG.replace_all(template, |caps: &Captures| match caps.extract() {
            (_, [field_name]) if field_name == "album_artist" => tag(&Standard(AlbumArtist)),
            (_, [field_name]) if field_name == "artist" => tag(&Standard(Artist)),
            (_, [field_name]) if field_name == "album" => tag(&Standard(Album)),
            (_, [field_name]) if field_name == "title" => tag(&Standard(TrackTitle)),
            (_, [field_name]) if field_name == "track_number" => {
                let nr = tag(&Standard(TrackNumber));
                nr.split('/').next().unwrap_or("00").to_owned()
            }
            (_, [field_name]) if field_name == "track_total" => tag(&Standard(TrackTotal)),
            (_, [field_name]) if field_name == "disk_number" => tag(&Standard(DiscNumber)),
            (_, [field_name]) if field_name == "disk_total" => tag(&Standard(DiscTotal)),
            (full, [field_name]) => panic!("field name {field_name} not recognized ({full})"),
        });
        Ok(PathBuf::from(
            path.into_owned()
                + "."
                + self
                    .path
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("no_ext"),
        ))
    }

    /// `file` HAS to be a media file
    pub fn parse_with_symphonia(file: impl AsRef<Path>) -> anyhow::Result<Self> {
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

            // If the packet does not belong to the selected track, skip over it.
            if packet.track_id() != track_id {
                continue;
            }

            // Decode the packet into audio samples.
            /*match decoder.decode(&packet) {
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
            }*/
        }

        // Consume every metadata revision in sequence
        loop {
            // update HashMap
            if let Some(metadata) = metadata_log.current() {
                metadata.tags().iter().for_each(|tag| {
                    let key = if let Some(key) = tag.std_key {
                        TagKey::Standard(key)
                    } else {
                        TagKey::Custom(tag.key.clone())
                    };
                    let value = tag.value.clone();
                    let old_value = tags.0.insert(key.clone(), value.clone());
                    if let Some(old_value) = old_value {
                        debug!("{file:?}: tag override on {key:?} ({old_value} => {value})");
                    } else {
                        debug!("{file:?}: found new tag {key:?} ({value:?})");
                    }
                })
            }

            if !metadata_log.is_latest() {
                // pop DOESN'T REMOVE THE LAST ONE so we have to use this ugly construct
                metadata_log.pop();
            } else {
                break;
            }
        }

        Ok(Self {
            tags,
            path: file.to_owned(),
        })
    }
}

pub fn is_media_file_by_name(file: impl AsRef<Path>) -> bool {
    let file = file.as_ref();
    let ext = file.extension().and_then(OsStr::to_str).unwrap_or("");

    // `is_file()` is true for symlinks (=_=)
    file.is_file() && !file.is_symlink() && MEDIA_EXTENSIONS.contains(&ext)
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use assert2::check;
    use walkdir::WalkDir;

    use crate::test::*;

    use super::*;
    #[test]
    fn test_is_media_file_by_name() -> Result<()> {
        let test_files_wheel_nonmedia = [
            "00-wheel-resident_human-cd-2021.m3u",
            "00-wheel-resident_human-cd-2021.nfo",
            "00-wheel-resident_human-cd-2021-proof.jpg",
        ];
        let test_files_wheel_media = [
            "01-wheel-dissipating.mp3",
            "02-wheel-movement.mp3",
            "03-wheel-ascend.mp3",
            "04-wheel-hyperion.mp3",
            "05-wheel-fugue.mp3",
            "06-wheel-resident_human.mp3",
            "07-wheel-old_earth.mp3",
        ];

        fn realpath(file: impl AsRef<Path>) -> PathBuf {
            TESTDATA_PATH
                .join("Wheel-Resident_Human-CD-2021-D2H")
                .join(file)
        }

        for file in test_files_wheel_media {
            let file = realpath(file);
            // sanity
            check!(file.exists());
            check!(is_media_file_by_name(realpath(file)) == true);
        }
        for file in test_files_wheel_nonmedia {
            let file = realpath(file);
            // sanity
            check!(file.exists());
            check!(is_media_file_by_name(realpath(file)) == false);
        }
        // TODO test more media and non-media

        for file in ["symlink.mp3", "symlink_broken.mp3", "dir"] {
            let file = TESTDATA_PATH.join("non-media").join(file);
            // sanity
            // also, `file.exists()` is false for broken symlinks (=_=)
            check!(file.exists() || file.is_symlink());
            check!(is_media_file_by_name(file) == false);
        }
        Ok(())
    }
}
