package util

func TernaryOperate[T int | int32 | string](condition bool, a, b T) T {
	if condition {
		return a
	}
	return b
}

func Max[T int8 | uint8 | uint16 | int | int32 | int64 | string | float32 | float64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func Min[T int8 | uint8 | uint16 | int | int32 | int64 | string | float32 | float64](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func ContainsInt(list []int, target int) bool {
	for _, e := range list {
		if e == target {
			return true
		}
	}

	return false
}

// todo 暂未实验
func Contains[T comparable](list []T, target T) bool {
	for _, e := range list {
		if e == target {
			return true
		}
	}

	return false
}

// Last 返回切片最后一个元素，如果切片为空则返回零值
func Last[T any](list []T) T {
	if len(list) == 0 {
		var zero T
		return zero
	}

	return list[len(list)-1]
}
