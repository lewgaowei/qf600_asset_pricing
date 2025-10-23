"""Remove emojis from Python script for Windows compatibility"""
import re

# Read the file
with open('03-machine-learning-lstm.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Common emoji replacements
emoji_map = {
    '✅': '[OK]',
    '❌': '[X]',
    '⚠️': '[WARN]',
    '🚀': '[GO]',
    '💻': '[CPU]',
    '📍': '[DEVICE]',
    '🧪': '[TEST]',
    '🔄': '[INFO]',
    '⚙️': '[CONFIG]',
    '📋': '[INFO]',
    '💾': '[SAVE]',
    '📊': '[STATS]',
    '🔍': '[CHECK]',
    '🏆': '[BEST]',
    '📚': '[INFO]',
    '💡': '[TIP]',
    '⚡': '[FAST]',
    '📈': '[UP]',
    '📉': '[DOWN]',
    '🎯': '[TARGET]',
    '🎉': '[SUCCESS]',
    '⏱️': '[TIME]',
    '⏭️': '[SKIP]',
    '🤖': '[AI]',
}

# Replace known emojis
for emoji, replacement in emoji_map.items():
    content = content.replace(emoji, replacement)

# Remove any remaining emojis (anything outside ASCII and common Latin characters)
# Keep printable ASCII + some extended Latin
emoji_pattern = re.compile('[^\x00-\x7F\u00A0-\u024F]+')
content = emoji_pattern.sub('', content)

# Write back
with open('03-machine-learning-lstm.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("Emojis removed successfully!")
