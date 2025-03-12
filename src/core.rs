//! This module contains the core functionality of the PGN parser, and does not reference Python.
//! The core functionality, along with unit tests, is implemented in Rust to ensure correctness.
//! The Python bindings are implemented in the `lib.rs` file.

use std::ops::Index;

use rayon::prelude::*;
use regex::Regex;

/// An enum representing the output of a regex compilation.
pub enum RegexResult {
    Compiled(Regex),
    Error(regex::Error),
}

impl RegexResult {
    /// Returns the compiled regex if successful, or the error if compilation failed.
    pub fn unwrap(self) -> Result<Regex, regex::Error> {
        match self {
            Self::Compiled(re) => Ok(re),
            Self::Error(e) => Err(e),
        }
    }

    /// Returns the compiled regex if successful, or panics with the error message if compilation failed.
    pub fn expect(self, msg: &str) -> Regex {
        match self {
            Self::Compiled(re) => re,
            Self::Error(e) => panic!("{}: {}", msg, e),
        }
    }

    pub fn captures<'t>(&self, text: &'t str) -> Option<regex::Captures<'t>> {
        match self {
            Self::Compiled(re) => re.captures(text),
            Self::Error(_) => None,
        }
    }
}

/// Returns a compiled regex that matches PGN games.
///
/// The regex is used to capture a PGN game block that begins with `[Event` and ends
/// with a game result (e.g., "1-0", "0-1", or "1/2-1/2").
///
/// # Errors
///
/// Returns a `regex::Error` if the pattern is invalid.
pub fn get_regex() -> RegexResult {
    match Regex::new(r"(?s)(\[Event.*?(?:1-0|0-1|1/2-1/2))") {
        Ok(re) => RegexResult::Compiled(re),
        Err(e) => RegexResult::Error(e),
    }
}

/// Returns a vector of PGN game blocks extracted from a larger PGN data string.
///
/// The provided regex should be capable of matching a complete PGN game block.
///
/// # Arguments
///
/// * `data` - A string slice containing one or more PGN games.
/// * `re` - A reference to a compiled regex for capturing game blocks.
///
/// # Examples
///
/// ```
/// let re = get_regex().unwrap();
/// let games = get_games(pgn_data, &re);
/// ```
pub fn get_games(data: &str, re: &Regex) -> Vec<String> {
    re.captures_iter(data)
        .collect::<Vec<_>>()
        .par_iter()
        .filter_map(|cap| cap.get(0).map(|m| m.as_str().to_string()))
        .collect()
}

/// Represents a PGN tag, formatted as `[TagName "TagValue"]`.
#[derive(Debug, PartialEq)]
pub struct Tag {
    pub name: String,
    pub value: String,
}

impl Tag {
    /// Creates a new `Tag` with the given name and value.
    ///
    /// # Arguments
    ///
    /// * `name` - The tag name.
    /// * `value` - The tag value.
    ///
    /// # Examples
    ///
    /// ```
    /// let tag = Tag::new("Event", "Test Game");
    /// ```
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// Creates a new `Tag` by parsing a PGN tag string.
    ///
    /// Note: This method is a placeholder because the provided regex is designed for whole games.
    /// A dedicated parser for single tags might be introduced in the future.
    ///
    /// # Arguments
    ///
    /// * `tag` - A string slice containing a PGN tag.
    pub fn from_pgn_string(tag: &str) -> Self {
        let re = get_regex().expect("Failed to compile regex");

        let cap = re.captures(tag).unwrap();
        // This is a placeholder implementation.
        Self::new(cap.get(1).unwrap().as_str(), cap.get(2).unwrap().as_str())
    }
}

/// Extracts all tags from a PGN game.
///
/// Tags are expected to be on their own lines, each starting with `[`.
///
/// # Arguments
///
/// * `game` - A string slice containing the full PGN game text.
///
/// # Examples
///
/// ```
/// let tags = extract_tags_rs(pgn_game);
/// assert_eq!(tags[0], Tag::new("Event", "Test Game 1"));
/// ```
pub fn extract_tags_rs(game: &str) -> Vec<Tag> {
    let re = Regex::new(r#"\[(\w+)\s+"(.*?)"\]"#).unwrap();
    re.captures_iter(game)
        .collect::<Vec<_>>()
        .par_iter()
        .map(|cap| Tag::new(cap.get(1).unwrap().as_str(), cap.get(2).unwrap().as_str()))
        .collect()
}

/// Extracts the moves section from a PGN game in a robust manner.
///
/// Instead of assuming a single blank line separates tags and moves, this function
/// scans the input line by line. Any line that does not start with `[` (after trimming)
/// is considered part of the moves section. It then:
///   - Joins these lines into a single string,
///   - Removes comments (enclosed in `{}`),
///   - Normalizes whitespace, and
///   - Removes any trailing game result token (e.g., "1-0", "0-1", "1/2-1/2", or "*").
///
/// The resulting move string is returned as a `Tag` with the name "Moves".
///
/// # Arguments
///
/// * `game` - A string slice containing the full PGN game text.
///
/// # Examples
///
/// ```
/// let moves = extract_moves_rs(pgn_game);
/// assert_eq!(moves, Tag::new("Moves", "1. e4 e5"));
/// ```
pub fn extract_moves_rs(game: &str) -> Tag {
    // Collect all lines that do not start with '[' (i.e. not tags)
    let moves_str: String = game
        .lines()
        .filter(|line| !line.trim_start().starts_with('['))
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<&str>>()
        .join(" ");

    // Remove comments enclosed in { }.
    let re_comment = Regex::new(r"\{[^}]*\}").unwrap();
    let moves_no_comments = re_comment.replace_all(&moves_str, "");

    // Normalize whitespace to a single space.
    let re_whitespace = Regex::new(r"\s+").unwrap();
    let normalized_moves = re_whitespace
        .replace_all(&moves_no_comments, " ")
        .to_string();
    let normalized_moves = normalized_moves.trim();

    // Remove a trailing game result token if present.
    let tokens: Vec<&str> = normalized_moves.split_whitespace().collect();
    let result_tokens = ["1-0", "0-1", "1/2-1/2", "*"];
    let final_moves = if let Some(last) = tokens.last() {
        if result_tokens.contains(last) {
            tokens[..tokens.len() - 1].join(" ")
        } else {
            normalized_moves.to_string()
        }
    } else {
        normalized_moves.to_string()
    };

    Tag::new("Moves", final_moves.trim())
}

/// Extracts the result tag from a PGN game.
///
/// The result tag is expected to be the last untagged statement in the game,
/// and is one of the following: "1-0", "0-1", "1/2-1/2", or "*", for white win, 
/// black win, draw, or undecided, respectively.
/// This function returns the string as a `Tag` with the name "Result", with value
/// that result token of "1-0", "0-1", "1/2-1/2", or "*".
pub fn extract_result_rs(game: &str) -> Tag {
    let re = Regex::new(r#"(\s+)([0\-1|1\-0|1/2\-1/2])"#).unwrap();
    // Find the last result token in the game.
    let result = re
        .captures_iter(game)
        .last()
        .map(|cap| cap.get(2).unwrap().as_str())
        .unwrap_or("*");
    Tag::new("Result", result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_get_regex() {
        let re = get_regex().expect("Failed to compile regex");
        let sample = "[Event \"Test Game\"]\n[Site \"?\"]\n\n1. e4 e5 1-0";
        assert!(re.is_match(sample));
    }

    #[test]
    fn can_get_games_from_string() {
        let re = get_regex().expect("Failed to compile regex");
        let sample = r#"[Event "Test Game 1"]
[Site "?"]

1. e4 e5 1-0

[Event "Test Game 2"]
[Site "?"]

1. e4 e5 0-1

[Event "Test Game 3"]
[Site "?"]

1. e4 e5 1/2-1/2

[Event "Test Game 4"]
[Site "?"]
[Third "Tagged Data"]

1. e4 e5 2. d4 d5 1-0

"#;
        let games = get_games(sample, &re);

        assert_eq!(games.len(), 4);
        assert_eq!(
            games[0],
            "[Event \"Test Game 1\"]\n[Site \"?\"]\n\n1. e4 e5 1-0"
        );
        assert_eq!(
            games[1],
            "[Event \"Test Game 2\"]\n[Site \"?\"]\n\n1. e4 e5 0-1"
        );
        assert_eq!(
            games[2],
            "[Event \"Test Game 3\"]\n[Site \"?\"]\n\n1. e4 e5 1/2-1/2"
        );
        assert_eq!(games[3], "[Event \"Test Game 4\"]\n[Site \"?\"]\n[Third \"Tagged Data\"]\n\n1. e4 e5 2. d4 d5 1-0");
    }

    #[test]
    fn can_extract_tags() {
        let game = r#"[Event "Test Game 1"]
[Site "?"]

1. e4 e5 1-0"#;
        let tags = extract_tags_rs(game);
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], Tag::new("Event", "Test Game 1"));
        assert_eq!(tags[1], Tag::new("Site", "?"));
    }

    #[test]
    fn can_extract_multiple_tags() {
        let game = r#"[Event "Test Game 1"]
[Site "?"]
[Date "2021.01.01"]
[Round "1"]
[White "White Player"]
[Black "Black Player"]
[Result "1-0"]

1. e4 e5 1-0"#;
        let tags = extract_tags_rs(game);
        assert_eq!(tags.len(), 7);
        assert_eq!(tags[0], Tag::new("Event", "Test Game 1"));
        assert_eq!(tags[1], Tag::new("Site", "?"));
        assert_eq!(tags[2], Tag::new("Date", "2021.01.01"));
        assert_eq!(tags[3], Tag::new("Round", "1"));
        assert_eq!(tags[4], Tag::new("White", "White Player"));
        assert_eq!(tags[5], Tag::new("Black", "Black Player"));
        assert_eq!(tags[6], Tag::new("Result", "1-0"));
    }

    #[test]
    fn can_extract_moves() {
        let game = r#"[Event "Test Game 1"]
[Site "?"]
[Date "2021.01.01"]
[Round "1"]
[White "White Player"]
[Black "Black Player"]
[Result "1-0"]

1. e4 e5 1-0"#;
        let moves = extract_moves_rs(game);
        assert_eq!(moves, Tag::new("Moves", "1. e4 e5"));
    }

    #[test]
    fn can_extract_moves_with_comments() {
        let game = r#"[Event "Test Game 1"]
[Site "?"]
[Date "2021.01.01"]

1. e4 {This is a comment} e5 1-0"#;
        let moves = extract_moves_rs(game);
        assert_eq!(moves, Tag::new("Moves", "1. e4 e5"));
    }

    #[test]
    fn can_extract_moves_with_variations() {
        let game = r#"[Event "Test Game 1"]
[Site "?"]
[Date "2021.01.01"]

1. e4 e5 (1... c5) 1-0"#;
        let moves = extract_moves_rs(game);
        assert_eq!(moves, Tag::new("Moves", "1. e4 e5 (1... c5)"));
    }

    #[test]
    fn can_extract_result() {
        let game = r#"[Event "Test Game 1"]
[Site "?"]
[Date "2021.01.01"]
[Round "1"]
[White "White Player"]
[Black "Black Player"]

1. e4 e5 1-0"#;
        let result = extract_result_rs(game);
        dbg!(&result);
        assert_eq!(result, Tag::new("Result", "1-0"));
    }

}
