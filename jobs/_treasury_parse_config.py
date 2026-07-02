import re, json
src = open('D:/research/econfindatalibrary/data/_treasury_catalog_chunk.js', encoding='utf-8').read()

# Show context around 'apis:' and 'endpoint:' to understand how the path is built.
# Find all "endpoint:" occurrences with surrounding context.
print('=== sample contexts around endpoint: ===')
for m in list(re.finditer(r'endpoint:', src))[:8]:
    i = m.start()
    print(repr(src[i-40:i+120]))
    print('---')

print('\n=== sample contexts around apiId ===')
for m in list(re.finditer(r'apiId', src))[:5]:
    i = m.start()
    print(repr(src[i-20:i+120]))
    print('---')

print('\n=== sample contexts around tableName ===')
for m in list(re.finditer(r'tableName:', src))[:5]:
    i = m.start()
    print(repr(src[i-20:i+160]))
    print('---')

# Look for the big array of dataset config (datasetId / slug + apis array)
print('\n=== contexts around "apis:" ===')
for m in list(re.finditer(r'apis:', src))[:4]:
    i = m.start()
    print(repr(src[i-60:i+260]))
    print('---')
