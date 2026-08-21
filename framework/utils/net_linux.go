package utils

import (
	"syscall"
)

func InitProcess() {
}

func SetSocketReuse(fd uintptr) {
	SetSockoptInt(fd, syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
}

func GetSockoptInt(fd uintptr, level, opt int) (int, error) {
	return syscall.GetsockoptInt(int(fd), level, opt)
}

func SetSockoptInt(fd uintptr, level, opt int, value int) (err error) {
	return syscall.SetsockoptInt(int(fd), level, opt, value)
}

func SetSockoptMin(fd uintptr, level, opt int, value int) (err error) {
	newValue := 0
	curSize, _ := syscall.GetsockoptInt(int(fd), level, opt)
	if curSize > 0 {
		if curSize < value {
			newValue = value
		}
	} else {
		newValue = value
	}
	if newValue > 0 {
		return syscall.SetsockoptInt(int(fd), level, opt, value)
	}
	return nil
}
