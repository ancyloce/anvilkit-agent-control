package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// SSLModeVerifyFull is the only sslmode a PostgreSQL DSN may name outside
// development (P0.6): the server's certificate chain and its host name are
// verified (against sslrootcert, or the system roots without one).
const SSLModeVerifyFull = "verify-full"

// CheckPostgresTLS is the data-plane TLS rule of a PostgreSQL DSN, in URL
// form (postgres://…?sslmode=…) or keyword/value form (… sslmode=…):
// outside development it must name sslmode=verify-full. Only what the DSN
// itself says counts (never PGSSLMODE or a service file), and every
// occurrence counts: a repeated key never hides a weaker mode, and the URL
// alias ssl=true reads as require. name labels the error; the DSN carries
// the password and is never echoed.
func CheckPostgresTLS(name, dsn string, development bool) error {
	if development {
		return nil
	}
	modes, err := postgresSSLModes(dsn)
	if err != nil {
		return fmt.Errorf("%s: not a parseable PostgreSQL DSN (URL or keyword/value form)", name)
	}
	if len(modes) == 0 {
		modes = []string{""}
	}
	for _, mode := range modes {
		if mode != SSLModeVerifyFull {
			return fmt.Errorf("%s: sslmode must be %s outside development (got %q)", name, SSLModeVerifyFull, mode)
		}
	}
	return nil
}

var errDSN = errors.New("unparseable DSN")

// postgresSSLModes returns every sslmode the DSN names, in order, following
// pgx's tokenization (pgconn.ParseConfig) of both forms.
func postgresSSLModes(dsn string) ([]string, error) {
	if strings.IndexByte(dsn, 0) >= 0 {
		return nil, errDSN
	}
	rest, ok := strings.CutPrefix(dsn, "postgresql://")
	if !ok {
		rest, ok = strings.CutPrefix(dsn, "postgres://")
	}
	if ok {
		return urlSSLModes(rest)
	}
	return keywordSSLModes(dsn)
}

// urlSSLModes reads the query of a postgres:// URL: like libpq, a '@'
// before the first '/' ends the userinfo, and the query follows the first
// '?' after it; pairs are key=value joined by '&', percent-encoded.
func urlSSLModes(rest string) ([]string, error) {
	if i := strings.IndexAny(rest, "@/"); i >= 0 && rest[i] == '@' {
		rest = rest[i+1:]
	}
	_, query, ok := strings.Cut(rest, "?")
	if !ok {
		return nil, nil
	}
	var modes []string
	for query != "" {
		var pair string
		pair, query, _ = strings.Cut(query, "&")
		rawKey, rawValue, found := strings.Cut(pair, "=")
		if !found || strings.Contains(rawValue, "=") {
			return nil, errDSN
		}
		key, err := url.PathUnescape(strings.Trim(rawKey, " "))
		if err != nil {
			return nil, errDSN
		}
		value, err := url.PathUnescape(strings.Trim(rawValue, " "))
		if err != nil {
			return nil, errDSN
		}
		switch {
		case key == "sslmode":
			modes = append(modes, value)
		case key == "ssl" && value == "true":
			modes = append(modes, "require")
		}
	}
	return modes, nil
}

const dsnSpace = " \t\n\r\v\f"

// keywordSSLModes reads a keyword/value DSN: key = value pairs separated by
// whitespace, a value either single-quoted or bare, '\' escaping the next
// byte in both.
func keywordSSLModes(s string) ([]string, error) {
	var modes []string
	s = strings.TrimLeft(s, dsnSpace)
	for s != "" {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return nil, errDSN
		}
		key := strings.Trim(s[:eq], dsnSpace)
		if key == "" || strings.ContainsAny(key, dsnSpace) {
			return nil, errDSN
		}
		s = strings.TrimLeft(s[eq+1:], dsnSpace)
		var value strings.Builder
		if strings.HasPrefix(s, "'") {
			end := 1
			for ; end < len(s) && s[end] != '\''; end++ {
				if s[end] == '\\' {
					if end++; end == len(s) {
						break
					}
				}
				value.WriteByte(s[end])
			}
			if end >= len(s) {
				return nil, errDSN
			}
			s = s[end+1:]
		} else {
			end := 0
			for ; end < len(s) && !strings.ContainsRune(dsnSpace, rune(s[end])); end++ {
				if s[end] == '\\' {
					if end++; end == len(s) {
						break
					}
				}
				value.WriteByte(s[end])
			}
			s = s[min(end, len(s)):]
		}
		s = strings.TrimLeft(s, dsnSpace)
		if key == "sslmode" {
			modes = append(modes, value.String())
		}
	}
	return modes, nil
}
