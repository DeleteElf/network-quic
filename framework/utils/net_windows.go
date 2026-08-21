package utils

/*
#cgo windows LDFLAGS: -lWinmm

#include <windows.h>

int SetTimePeriod() {
    if (timeBeginPeriod(1) == TIMERR_NOERROR) {
        return 0;
    }
    return -1;
}
*/
import "C"

import (
	"log/slog"
	"syscall"
)

func InitProcess() {
	if C.SetTimePeriod() != 0 {
		slog.Info("SetTimePeriod fail")
	} else {
		slog.Info("SetTimePeriod")
	}
}

func SetSocketReuse(fd uintptr) {
	SetSockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
}

func GetSockoptInt(fd uintptr, level, opt int) (int, error) {
	return syscall.GetsockoptInt(syscall.Handle(fd), level, opt)
}

func SetSockoptInt(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, opt, value)
}

func SetSockoptMin(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(syscall.Handle(fd), level, opt, value)
}
