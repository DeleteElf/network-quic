package utils

import (
	"golang.org/x/exp/constraints"
	"math"
)

// CeilUint64 计算a/b取上整
func CeilUint64(a, b uint64) uint64 {
	return (a + b - 1) / b
}

// Ceil 计算a/b取上整
func Ceil[T constraints.Integer](a, b T) T {
	return T(CeilUint64(uint64(a), uint64(b))) //为了防止数据溢出，统一使用uint64进行计算再还原单位
}

// IsBefore 判断a是否比b小，支持数据溢出后的计算
func IsBefore[T constraints.Integer](a, b, max T) bool {
	return T(int(a)-int(b)) > max/2
}

// IsBefore8 判断a是否比b小，支持数据溢出后的计算，如 255 比较 256
func IsBefore8(a, b uint8) bool {
	return IsBefore(a, b, math.MaxUint8)
}

// IsBefore16 判断a是否比b小，支持数据溢出后的计算，如 65535 比较 65536
func IsBefore16(a, b uint16) bool {
	return IsBefore(a, b, math.MaxUint16)
}
