package utils

import "golang.org/x/exp/constraints"

// CeilUint64 计算a/b取上整
func CeilUint64(a, b uint64) uint64 {
	return (a + b - 1) / b
}

// Ceil 计算a/b取上整
func Ceil[T constraints.Integer](a, b T) T {
	return T(CeilUint64(uint64(a), uint64(b))) //为了防止数据溢出，统一使用uint64进行计算再还原单位
}
