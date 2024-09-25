### sort와 sorted의 차이점
a1 = [6, 3, 9, 5]
print('a1:', a1)
a2 = a1.sort()
print('-----정렬 후-----')
print('a1:', a1)
print('a2:', a2)

# - sort 함수는 리스트 원본값을 직접 수정

print('=======================')
b1 = [6, 3, 9, 5]
print('b1:', b1)
b2 = sorted(b1)
print('-----정렬 후-----')
print('b1:', b1)
print('b2:', b2)

# - sorted 함수는 리스트 원본 값은 그대로이고 정렬 값을 반환