"""Test PyTorch GPU with RTX 5090"""
import torch
import torch.nn as nn
import time

print("="*60)
print("PYTORCH RTX 5090 PERFORMANCE TEST")
print("="*60)

print(f"\nGPU: {torch.cuda.get_device_name(0)}")
print(f"Compute Capability: sm_{torch.cuda.get_device_capability()[0]}{torch.cuda.get_device_capability()[1]}")

# Test if GPU operations work
try:
    print("\n" + "-"*60)
    print("Testing basic GPU operations...")
    print("-"*60)

    # Create tensors on GPU
    x = torch.randn(1000, 1000, device='cuda')
    y = torch.randn(1000, 1000, device='cuda')

    # Matrix multiplication on GPU
    start = time.time()
    z = torch.matmul(x, y)
    torch.cuda.synchronize()
    gpu_time = time.time() - start

    print(f"GPU matrix multiply (1000x1000): {gpu_time*1000:.2f}ms")
    print("OK GPU operations work!")

    # Test LSTM on GPU
    print("\n" + "-"*60)
    print("Testing LSTM on GPU...")
    print("-"*60)

    class SimpleLSTM(nn.Module):
        def __init__(self):
            super().__init__()
            self.lstm = nn.LSTM(input_size=50, hidden_size=64, batch_first=True)
            self.fc = nn.Linear(64, 1)

        def forward(self, x):
            out, _ = self.lstm(x)
            out = self.fc(out[:, -1, :])
            return out

    model = SimpleLSTM().cuda()
    x_test = torch.randn(32, 10, 50, device='cuda')

    start = time.time()
    output = model(x_test)
    torch.cuda.synchronize()
    lstm_time = time.time() - start

    print(f"LSTM forward pass (32 samples): {lstm_time*1000:.2f}ms")
    print(f"Output shape: {output.shape}")
    print("OK LSTM works on GPU!")

    print("\n" + "="*60)
    print("SUCCESS! GPU operations work despite the warning!")
    print("Your RTX 5090 can be used for training.")
    print("="*60)

except Exception as e:
    print(f"\nERROR: {e}")
    print("\nGPU operations failed. Falling back to CPU mode.")
    print("="*60)
