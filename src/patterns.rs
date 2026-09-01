use globset::{GlobBuilder, GlobMatcher};

use crate::error::{AppError, AppResult};

#[derive(Debug)]
pub struct PatternSet {
    positives: Vec<Pattern>,
    negatives: Vec<Pattern>,
}

#[derive(Debug)]
enum Pattern {
    Exact(String),
    Glob(GlobMatcher),
}

impl PatternSet {
    pub fn compile(values: &[String]) -> AppResult<Self> {
        let mut positives = Vec::new();
        let mut negatives = Vec::new();
        for value in values {
            let (negative, body) = match value.strip_prefix('!') {
                Some(body) => (true, body),
                None => (false, value.as_str()),
            };
            let pattern = compile_pattern(body)?;
            if negative {
                negatives.push(pattern);
            } else {
                positives.push(pattern);
            }
        }
        Ok(Self {
            positives,
            negatives,
        })
    }

    pub fn selects(&self, path: &str) -> bool {
        let included =
            self.positives.is_empty() || self.positives.iter().any(|pattern| pattern.matches(path));
        included && !self.negatives.iter().any(|pattern| pattern.matches(path))
    }
}

impl Pattern {
    fn matches(&self, path: &str) -> bool {
        match self {
            Self::Exact(expected) => path == expected || is_descendant(path, expected),
            Self::Glob(matcher) => {
                matcher.is_match(path) || ancestors(path).any(|ancestor| matcher.is_match(ancestor))
            }
        }
    }
}

fn compile_pattern(value: &str) -> AppResult<Pattern> {
    let value = value
        .strip_prefix("./")
        .unwrap_or(value)
        .trim_end_matches('/');
    if value.is_empty() {
        return Err(AppError::message("download patterns cannot be empty"));
    }
    validate_pattern_path(value)?;
    if has_glob_metacharacter(value) {
        let glob = GlobBuilder::new(value)
            .literal_separator(true)
            .build()
            .map_err(|error| {
                AppError::message(format!("invalid glob pattern {value:?}: {error}"))
            })?;
        Ok(Pattern::Glob(glob.compile_matcher()))
    } else {
        Ok(Pattern::Exact(value.to_owned()))
    }
}

fn validate_pattern_path(value: &str) -> AppResult<()> {
    if value.starts_with('/') {
        return Err(AppError::message(format!(
            "pattern {value:?} must be repository-relative"
        )));
    }
    if value
        .split('/')
        .any(|component| component == "." || component == "..")
    {
        return Err(AppError::message(format!(
            "pattern {value:?} contains an unsafe path component"
        )));
    }
    Ok(())
}

fn has_glob_metacharacter(value: &str) -> bool {
    value
        .chars()
        .any(|character| matches!(character, '*' | '?' | '[' | '{'))
}

fn is_descendant(path: &str, parent: &str) -> bool {
    path.strip_prefix(parent)
        .is_some_and(|remainder| remainder.starts_with('/'))
}

fn ancestors(path: &str) -> impl Iterator<Item = &str> {
    path.match_indices('/').map(|(index, _)| &path[..index])
}

#[cfg(test)]
mod tests {
    use super::PatternSet;

    fn patterns(values: &[&str]) -> PatternSet {
        PatternSet::compile(
            &values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    #[test]
    fn no_patterns_selects_everything() {
        assert!(patterns(&[]).selects("weights/model.safetensors"));
    }

    #[test]
    fn positive_patterns_form_a_union() {
        let set = patterns(&["README.md", "configs/*.json"]);
        assert!(set.selects("README.md"));
        assert!(set.selects("configs/model.json"));
        assert!(!set.selects("weights/model.safetensors"));
    }

    #[test]
    fn negative_patterns_subtract_regardless_of_order() {
        let first = patterns(&["!tests/**", "**/*.json"]);
        let second = patterns(&["**/*.json", "!tests/**"]);
        assert!(first.selects("configs/model.json"));
        assert!(second.selects("configs/model.json"));
        assert!(!first.selects("tests/model.json"));
        assert!(!second.selects("tests/model.json"));
    }

    #[test]
    fn only_negative_patterns_start_from_everything() {
        let set = patterns(&["!**/*.safetensors"]);
        assert!(set.selects("README.md"));
        assert!(!set.selects("weights/model.safetensors"));
    }

    #[test]
    fn exact_directory_selects_descendants() {
        let set = patterns(&["tokenizer/"]);
        assert!(set.selects("tokenizer/config.json"));
        assert!(!set.selects("tokenizer.json"));
    }

    #[test]
    fn globbed_directory_selects_descendants() {
        let set = patterns(&["models/*"]);
        assert!(set.selects("models/v1/config.json"));
    }

    #[test]
    fn rejects_parent_components() {
        let result = PatternSet::compile(&["../secret".to_owned()]);
        assert!(result.is_err());
    }
}
