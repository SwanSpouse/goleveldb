// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"leveldb/cache"
	"leveldb/comparer"
	"leveldb/filter"
	"leveldb/opt"
)

type iOptions struct {
	opt.Options
	s *session
}

func newIOptions(s *session, o opt.Options) *iOptions {
	p := &iOptions{Options: o, s: s}
	p.sanitize()
	return p
}

func (o *iOptions) sanitize() {
	if p := o.GetBlockCache(); p == nil {
		o.Options.SetBlockCache(cache.NewLRUCache(opt.DefaultBlockCacheSize))
	}

	if p := o.GetFilter(); p != nil {
		o.Options.SetFilter(&iFilter{p})
	}
}

func (o *iOptions) GetComparer() comparer.Comparer {
	return o.s.cmp
}

func (o *iOptions) SetComparer(cmp comparer.Comparer) error {
	return opt.ErrNotAllowed
}

func (o *iOptions) SetMaxOpenFiles(max int) error {
	err := o.Options.SetMaxOpenFiles(max)
	if err != nil {
		return err
	}
	o.s.tops.cache.SetCapacity(max)
	return nil
}

func (o *iOptions) SetBlockCache(cache cache.Cache) error {
	err := o.Options.SetBlockCache(cache)
	if err != nil {
		return err
	}
	o.s.tops.cache.Purge(nil)
	return nil
}

func (o *iOptions) SetFilter(p filter.Filter) error {
	if p != nil {
		p = &iFilter{p}
	}
	return o.Options.SetFilter(p)
}
