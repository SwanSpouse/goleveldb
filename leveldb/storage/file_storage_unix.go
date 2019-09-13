// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// +build darwin dragonfly freebsd linux netbsd openbsd

package storage

import (
	"os"
	"syscall"
)

type unixFileLock struct {
	f *os.File
}

// 释放文件锁
func (fl *unixFileLock) release() error {
	if err := setFileLock(fl.f, false, false); err != nil {
		return err
	}
	return fl.f.Close()
}

// 创建一个文件锁
func newFileLock(path string, readOnly bool) (fl fileLock, err error) {
	var flag int
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_RDWR
	}
	// 创建一个文件
	f, err := os.OpenFile(path, flag, 0)
	if os.IsNotExist(err) {
		f, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return
	}
	// 创建文件锁
	// 这里会写入一些文件描述符，这样就保证了在进程终止或者文件句柄被释放的时候，文件锁可以被释放。
	err = setFileLock(f, readOnly, true)
	if err != nil {
		f.Close()
		return
	}
	// TODO limingji 这里为啥是unixFileLock呢？
	// 你打开其他文件的时候ide会提示你说你打开的文件和系统不符
	fl = &unixFileLock{f: f}
	return
}

/*
flock主要三种操作类型：
	LOCK_SH，共享锁，多个进程可以使用同一把锁，常被用作读共享锁；
	LOCK_EX，排他锁，同时只允许一个进程使用，常被用作写锁；
	LOCK_UN，释放锁；
*/
func setFileLock(f *os.File, readOnly, lock bool) error {
	how := syscall.LOCK_UN
	if lock {
		if readOnly {
			// 只读当然是共享锁
			how = syscall.LOCK_SH
		} else {
			// 如果要是往文件里写入，当然是排他锁
			how = syscall.LOCK_EX
		}
	}
	// flock 是对于整个文件的建议性锁。也就是说，如果一个进程在一个文件（inode）上放了锁，那么其它进程是可以知道的。
	// （建议性锁不强求进程遵守。） 最棒的一点是，它的第一个参数是文件描述符，在此文件描述符关闭时，锁会自动释放。
	// 而当进程终止时，所有的文件描述符均会被关闭。所以很多时候就不用考虑类似原子锁解锁的事情。
	return syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB)
}

func rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func isErrInvalid(err error) bool {
	if err == os.ErrInvalid {
		return true
	}
	// Go < 1.8
	if syserr, ok := err.(*os.SyscallError); ok && syserr.Err == syscall.EINVAL {
		return true
	}
	// Go >= 1.8 returns *os.PathError instead
	if patherr, ok := err.(*os.PathError); ok && patherr.Err == syscall.EINVAL {
		return true
	}
	return false
}

func syncDir(name string) error {
	// As per fsync manpage, Linux seems to expect fsync on directory, however
	// some system don't support this, so we will ignore syscall.EINVAL.
	//
	// From fsync(2):
	//   Calling fsync() does not necessarily ensure that the entry in the
	//   directory containing the file has also reached disk. For that an
	//   explicit fsync() on a file descriptor for the directory is also needed.
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !isErrInvalid(err) {
		return err
	}
	return nil
}
