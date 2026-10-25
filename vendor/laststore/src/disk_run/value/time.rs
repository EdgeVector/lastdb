//! UTC timestamp text `YYYY-MM-DDTHH:MM:SS[.f]Z` as numbers, for years 0 to
//! 9999 and 0 to 9 fraction digits.

/// Seconds since 1970, the count of fraction digits, and the fraction of a
/// timestamp text with a real date and time. A leap second is not one.
pub(super) fn parse(text: &[u8]) -> Option<(i64, u8, u64)> {
    if !(20..=30).contains(&text.len()) || text.last() != Some(&b'Z') {
        return None;
    }
    let places = [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')];
    if places.iter().any(|&(at, byte)| text[at] != byte) {
        return None;
    }
    let number = |digits: &[u8]| {
        digits.iter().try_fold(0u64, |value, &digit| {
            digit
                .is_ascii_digit()
                .then(|| value * 10 + u64::from(digit - b'0'))
        })
    };
    let field = |at: usize, len: usize| number(&text[at..at + len]).map(|value| value as i64);
    let (year, month, day) = (field(0, 4)?, field(5, 2)?, field(8, 2)?);
    let (hour, minute, second) = (field(11, 2)?, field(14, 2)?, field(17, 2)?);
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let (digits, fraction) = match &text[19..text.len() - 1] {
        [] => (0, 0),
        [b'.', digits @ ..] if (1..=9).contains(&digits.len()) => {
            (digits.len() as u8, number(digits)?)
        }
        _ => return None,
    };
    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    Some((seconds, digits, fraction))
}

/// The timestamp text of [`parse`] output, or `None` when the parts do not
/// name one.
pub(super) fn format(seconds: i64, digits: u8, fraction: u64) -> Option<Vec<u8>> {
    if digits > 9 || fraction >= 10u64.pow(u32::from(digits)) {
        return None;
    }
    let (days, rest) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    if !(0..=9999).contains(&year) {
        return None;
    }
    let mut text = format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        rest / 3_600,
        rest / 60 % 60,
        rest % 60
    );
    if digits > 0 {
        text.push_str(&format!(".{fraction:0width$}", width = usize::from(digits)));
    }
    text.push('Z');
    Some(text.into_bytes())
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's
/// `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let (era, of_era) = (year.div_euclid(400), year.rem_euclid(400));
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = of_era * 365 + of_era / 4 - of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The date of a day count from 1970-01-01 (H. Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let (era, day_of_era) = (days.div_euclid(146_097), days.rem_euclid(146_097));
    let of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * of_era + of_era / 4 - of_era / 100);
    let shifted = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted + 2) / 5 + 1;
    let month = if shifted < 10 { shifted + 3 } else { shifted - 9 };
    (of_era + era * 400 + i64::from(month <= 2), month, day)
}
