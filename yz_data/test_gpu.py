"""
Quick GPU Test for TensorFlow + RTX 5090
Run this to verify your GPU is detected and working
"""

import tensorflow as tf
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

print("="*60)
print("TENSORFLOW GPU TEST")
print("="*60)

# 1. Check TensorFlow version
print(f"\nTensorFlow Version: {tf.__version__}")

# 2. Check if GPU is available
print("\n" + "-"*60)
print("GPU DETECTION:")
print("-"*60)

gpus = tf.config.list_physical_devices('GPU')
if gpus:
    print(f"✅ {len(gpus)} GPU(s) detected!")
    for i, gpu in enumerate(gpus):
        print(f"\n   GPU {i}:")
        print(f"   - Name: {gpu.name}")
        print(f"   - Type: {gpu.device_type}")

        # Try to get more details
        try:
            details = tf.config.experimental.get_device_details(gpu)
            if details:
                print(f"   - Device: {details.get('device_name', 'Unknown')}")
                print(f"   - Compute Capability: {details.get('compute_capability', 'Unknown')}")
        except:
            print(f"   - Additional details not available")

    # Enable memory growth
    try:
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)
        print(f"\n✅ Memory growth enabled (prevents OOM errors)")
    except:
        print(f"\n⚠️  Could not enable memory growth")
else:
    print("❌ No GPU detected!")
    print("\nTroubleshooting:")
    print("1. Check NVIDIA driver: nvidia-smi")
    print("2. Install CUDA Toolkit 12.x")
    print("3. Install cuDNN 8.9+")
    print("4. Reinstall TensorFlow: pip install tensorflow[and-cuda]")
    exit()

# 3. Test GPU with simple LSTM model
print("\n" + "-"*60)
print("GPU PERFORMANCE TEST:")
print("-"*60)

print("\nBuilding test LSTM model...")
# Create dummy data
X_train = np.random.randn(1000, 10, 50)  # 1000 samples, 10 timesteps, 50 features
y_train = np.random.randn(1000, 1)

# Build LSTM model
model = Sequential([
    LSTM(64, input_shape=(10, 50)),
    Dense(32, activation='relu'),
    Dense(1)
])

model.compile(optimizer='adam', loss='mse')

print("\n🚀 Training on GPU (this should be fast)...")
import time
start = time.time()

with tf.device('/GPU:0'):
    history = model.fit(X_train, y_train, epochs=5, batch_size=32, verbose=0)

end = time.time()
duration = end - start

print(f"✅ Training completed in {duration:.2f} seconds")
print(f"   Final loss: {history.history['loss'][-1]:.4f}")

if duration < 10:
    print(f"\n🎉 GPU is working! Training was fast ({duration:.1f}s for 5 epochs)")
    print(f"   Your RTX 5090 is properly configured!")
else:
    print(f"\n⚠️  Training was slow ({duration:.1f}s). GPU might not be used.")
    print(f"   Check CUDA installation")

# 4. Check CUDA availability
print("\n" + "-"*60)
print("CUDA DETAILS:")
print("-"*60)

print(f"CUDA available: {tf.test.is_built_with_cuda()}")
print(f"GPU available: {tf.test.is_gpu_available(cuda_only=False, min_cuda_compute_capability=None)}")

# 5. Memory info
print("\n" + "-"*60)
print("GPU MEMORY:")
print("-"*60)

try:
    memory_info = tf.config.experimental.get_memory_info('/GPU:0')
    print(f"Current memory usage: {memory_info['current'] / 1e9:.2f} GB")
    print(f"Peak memory usage: {memory_info['peak'] / 1e9:.2f} GB")
except:
    print("Memory info not available")

print("\n" + "="*60)
print("TEST COMPLETE!")
print("="*60)
