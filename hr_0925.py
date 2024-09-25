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

print('##########################################################################')

### 정렬의 종류..
'''
# 대표적인 정렬의 종류
- O(n²)의 시간 복잡도 (정렬할 자료의 수가 늘어나면 제곱에 비례해서 증가)
    버블 정렬(Bubble Sort)
    선택 정렬(Selection Sort)
    삽입 정렬(Insertion Sort)
- O(n log n)의 시간 복잡도
    병합 정렬(Merge Sort)
    퀵 정렬(Quick Sort)
'''