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
		if e== target {
			return true
		}
	}

	return false
}
