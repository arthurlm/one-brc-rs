use one_brc_rs::simd_buffers::SimdBuffer;

#[test]
fn test_usecase() {
    // Test single value
    let mut buffer = SimdBuffer::with_value(5);
    assert_eq!(buffer.len(), 1);
    assert!(!buffer.is_full());
    assert_eq!(buffer.as_slice(), &[5]);
    assert_eq!(buffer.min_max(), (5, 5));

    // Test with some data but not full.
    for i in 2..=14 {
        buffer.add(i);
    }
    assert_eq!(buffer.len(), 14);
    assert!(!buffer.is_full());
    assert_eq!(
        buffer.as_slice(),
        &[5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    );
    assert_eq!(buffer.min_max(), (2, 14));

    // Insert last element before full vec.
    buffer.add(15);
    assert_eq!(buffer.len(), 15);
    assert!(!buffer.is_full());
    assert_eq!(
        buffer.as_slice(),
        &[5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    );
    assert_eq!(buffer.min_max(), (2, 15));

    // Vector should not be full.
    buffer.add(16);
    assert_eq!(buffer.len(), 16);
    assert!(buffer.is_full());
    assert_eq!(
        buffer.as_slice(),
        &[5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    );
    assert_eq!(buffer.min_max(), (2, 16));

    // Cannot insert anything more in the vec.
    buffer.add(17);
    assert_eq!(buffer.len(), 16);
    assert!(buffer.is_full());
    assert_eq!(
        buffer.as_slice(),
        &[5, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    );
    assert_eq!(buffer.min_max(), (2, 16));
}
