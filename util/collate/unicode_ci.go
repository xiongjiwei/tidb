// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import (
	"strings"

	"github.com/pingcap/tidb/util/stringutil"
)

// unicodeCICollator is used for sort and compare unicode according to the Unicode Collation Algorithm (UCA)
// see also https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-sets.html#charset-unicode-sets-uca
type unicodeCICollator struct {
}

// Compare implement Collator interface.
func (uc *unicodeCICollator) Compare(a, b string) int {
	return strings.Compare(truncateTailingSpace(a), truncateTailingSpace(b))
}

// Key implements Collator interface.
func (uc *unicodeCICollator) Key(str string) []byte {
	return []byte(truncateTailingSpace(str))
}

// Pattern implements Collator interface.
func (uc *unicodeCICollator) Pattern() WildcardPattern {
	return &unicodePattern{}
}

// unicodePattern implementment use binaryParttern temporary
type unicodePattern struct {
	patChars []byte
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) Compile(patternStr string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePattern(patternStr, escape)
}

// Compile implements WildcardPattern interface.
func (p *unicodePattern) DoMatch(str string) bool {
	return stringutil.DoMatch(str, p.patChars, p.patTypes)
}
