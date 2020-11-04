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

import "github.com/pingcap/tidb/util/stringutil"

// Collation of utf8mb4_zh_pinyin_tidb_as_cs
type zhPinyinTiDBASCSCollator struct {
}

// Compare implements Collator interface.
func (py *zhPinyinTiDBASCSCollator) Compare(a, b string) int {
	a = truncateTailingSpace(a)
	b = truncateTailingSpace(b)

	ar, br := rune(0), rune(0)
	ai, bi := 0, 0

	for ai < len(a) && bi < len(b) {
		ar, ai = decodeRune(a, ai)
		br, bi = decodeRune(b, bi)

		cmp := int(convertRunePinyin(ar)) - int(convertRunePinyin(br))
		if cmp != 0 {
			return sign(cmp)
		}
	}

	return sign((len(a) - ai) - (len(b) - bi))
}

// Key implements Collator interface.
func (py *zhPinyinTiDBASCSCollator) Key(str string) []byte {
	str = truncateTailingSpace(str)
	buf := make([]byte, 0, len(str)*2)
	r, i := rune(0), 0

	for i < len(str) {
		r, i = decodeRune(str, i)
		k := convertRunePinyin(r)

		switch {
		case k <= 0xFF:
			buf = append(buf, byte(k))
		case k <= 0xFFFF:
			buf = append(buf, byte(k>>8), byte(k))
		default:
			buf = append(buf, byte(k>>24), byte(k>>16), byte(k>>8), byte(k))
		}
	}

	return buf
}

// Pattern implements Collator interface.
func (py *zhPinyinTiDBASCSCollator) Pattern() WildcardPattern {
	return &pinyinPattern{}
}

type pinyinPattern struct {
	patChars []rune
	patTypes []byte
}

// Compile implements WildcardPattern interface.
func (p *pinyinPattern) Compile(pattern string, escape byte) {
	p.patChars, p.patTypes = stringutil.CompilePatternInner(pattern, escape)
}

// DoMatch implements WildcardPattern interface.
func (p *pinyinPattern) DoMatch(str string) bool {
	return stringutil.DoMatchInner(str, p.patChars, p.patTypes, func(a, b rune) bool {
		return convertRunePinyin(a) == convertRunePinyin(b)
	})
}

func convertRunePinyin(r rune) uint32 {
	if r <= 0xFFFF {
		return zhPinyinTiDBASCSBMP[r]
	}

	if c, ok := zhPinyinTiDBASCSNoBMP[r]; ok {
		return c
	}

	return 0xFF000000 + uint32(r) + 0x1E248
}
