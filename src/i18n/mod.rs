use std::collections::BTreeMap;

use crate::domain::Language;

pub struct Catalog {
    language: Language,
    entries: BTreeMap<&'static str, &'static str>,
}

impl Catalog {
    pub fn new(language: Language) -> Self {
        let source = match language {
            Language::En => include_str!("../../locales/en.txt"),
            Language::ZhCn => include_str!("../../locales/zh-CN.txt"),
        };
        let entries = source
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect();
        Self { language, entries }
    }

    pub fn language(&self) -> Language {
        self.language
    }

    pub fn text<'a>(&'a self, key: &'a str) -> &'a str {
        self.entries.get(key).copied().unwrap_or(key)
    }
}
