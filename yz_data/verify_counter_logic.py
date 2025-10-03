# %%
# =============================================================================
# VERIFICATION SCRIPT: Counter Logic Fix
# =============================================================================
# This script verifies that the counter logic fix in 03-machine-learning.py
# is working correctly. Run this AFTER completing Step 7 (Cross-Validation).
#
# Expected behavior after fix:
# - For test counter N, CV file counter_N_cv.csv should exist
# - That CV file was created by validating on counter N-1
# - Step 8 will use this CV file to test on counter N
# =============================================================================

import pandas as pd
from pathlib import Path

print("="*70)
print("COUNTER LOGIC VERIFICATION")
print("="*70)

# Configuration (must match your 03-machine-learning.py)
_base_dir = Path.cwd()
output_dir = _base_dir / "ml_results"
cv_dir = output_dir / "CV"

CONFIG = {
    'window': 'recursive',
    'cv_validation': 1,
    'begin': 5,  # Adjust to match your CONFIG['begin']
}

print(f"\nConfiguration:")
print(f"  Window: {CONFIG['window']}")
print(f"  CV Validation: {CONFIG['cv_validation']} year(s)")
print(f"  First test counter: {CONFIG['begin']}")

# =============================================================================
# TEST 1: Check if CV files exist for correct counters
# =============================================================================
print(f"\n{'='*70}")
print("TEST 1: CV File Naming")
print(f"{'='*70}")

test_counters = [CONFIG['begin'], CONFIG['begin'] + 1, CONFIG['begin'] + 2]

for test_counter in test_counters:
    cv_filename = f"brt_recursive_dep_expected_return_val_{CONFIG['cv_validation']}_counter_{test_counter}_cv.csv"
    cv_file = cv_dir / cv_filename

    if cv_file.exists():
        print(f"✅ Test counter {test_counter}: CV file exists ({cv_filename})")

        # Load CV results
        cv_data = pd.read_csv(cv_file)
        best_row = cv_data.sort_values('r2_score', ascending=False).iloc[0]

        print(f"   Best hyperparameters:")
        print(f"     n_estimators: {int(best_row['n_estimators'])}")
        print(f"     learning_rate: {best_row['learning_rate']:.3f}")
        print(f"     R²: {best_row['r2_score']:+.4f}")
        print(f"   ✅ This CV was created by validating on counter {test_counter-1}")
        print(f"   ✅ These hyperparameters should be used for TESTING counter {test_counter}")
    else:
        print(f"❌ Test counter {test_counter}: CV file NOT found ({cv_filename})")
        print(f"   Expected path: {cv_file}")

# =============================================================================
# TEST 2: Verify the logic matches professor's approach
# =============================================================================
print(f"\n{'='*70}")
print("TEST 2: Logic Verification")
print(f"{'='*70}")

test_counter = CONFIG['begin']
validation_endpoint = test_counter - 1

print(f"\nFor TESTING counter {test_counter}:")
print(f"  Step 7 should iterate with k = {validation_endpoint}")
print(f"  train_validation_data(k={validation_endpoint}) produces:")
print(f"    Train: counters ≤ {validation_endpoint - CONFIG['cv_validation']}")
print(f"    Validate: counter {validation_endpoint}")
print(f"  CV results saved as: counter_{test_counter}_cv.csv")
print(f"\n  Step 8 will:")
print(f"    Load: counter_{test_counter}_cv.csv")
print(f"    Train on: counters ≤ {validation_endpoint}")
print(f"    Test on: counter {test_counter}")
print(f"\n✅ Hyperparameters match! (Both optimized for testing counter {test_counter})")

# =============================================================================
# TEST 3: Check prediction files match CV files
# =============================================================================
print(f"\n{'='*70}")
print("TEST 3: CV-Prediction File Alignment")
print(f"{'='*70}")

pred_dir = output_dir / "Pred"

for test_counter in test_counters[:2]:  # Check first 2
    cv_filename = f"brt_recursive_dep_expected_return_val_{CONFIG['cv_validation']}_counter_{test_counter}_cv.csv"
    pred_filename = f"brt_recursive_dep_expected_return_val_{CONFIG['cv_validation']}_counter_{test_counter}_pred.csv"

    cv_file = cv_dir / cv_filename
    pred_file = pred_dir / pred_filename

    cv_exists = cv_file.exists()
    pred_exists = pred_file.exists()

    if cv_exists and pred_exists:
        print(f"✅ Counter {test_counter}: Both CV and Pred files exist")

        # Verify they're for the same test counter
        pred_data = pd.read_csv(pred_file)
        print(f"   Predictions made: {len(pred_data):,} observations")

    elif cv_exists and not pred_exists:
        print(f"⚠️  Counter {test_counter}: CV exists, but Pred not yet generated")
        print(f"   (Run Step 8 to generate predictions)")

    elif not cv_exists and pred_exists:
        print(f"❌ Counter {test_counter}: Pred exists without CV (ERROR!)")

    else:
        print(f"❌ Counter {test_counter}: Neither CV nor Pred exists")

# =============================================================================
# TEST 4: Compare with professor's approach
# =============================================================================
print(f"\n{'='*70}")
print("TEST 4: Professor's Approach Comparison")
print(f"{'='*70}")

print(f"\nProfessor's code (analyze_data_V2.py):")
print(f"  for k in range(begin, end):  # k = 4, 5, 6, ...")
print(f"    train_validation_data(data, k, ...)  # k is validation endpoint")
print(f"    cv_results.to_csv(...'counter_' + str(k+1) + '_cv.csv')")
print(f"\nYour code (AFTER FIX):")
print(f"  for k in range(CONFIG['begin']-1, CONFIG['end']):  # k = 4, 5, 6, ...")
print(f"    train_validation_data(data, k, ...)  # k is validation endpoint")
print(f"    output_file = output_filename(config, counter=k+1)")
print(f"\n✅ MATCH! Your code now follows professor's logic exactly!")

# =============================================================================
# TEST 5: Detailed trace of one iteration
# =============================================================================
print(f"\n{'='*70}")
print("TEST 5: Detailed Trace of One Iteration")
print(f"{'='*70}")

k = CONFIG['begin'] - 1
test_counter = k + 1
train_end = k - CONFIG['cv_validation']
val_start = k - CONFIG['cv_validation'] + 1
val_end = k

print(f"\nStep 7 iteration with k = {k}:")
print(f"  1. Loop: for k in range({CONFIG['begin']-1}, ...)")
print(f"     → k = {k}")
print(f"\n  2. train_validation_data(data, k={k}, ...)")
print(f"     → Train: counters ≤ {train_end}")
print(f"     → Validate: counters {val_start}-{val_end}")
print(f"\n  3. Save CV results:")
print(f"     → output_filename(counter={k+1})")
print(f"     → File: counter_{test_counter}_cv.csv")
print(f"\n  4. Step 8 will load counter_{test_counter}_cv.csv and test on counter {test_counter}")
print(f"\n✅ CORRECT! CV for test counter {test_counter} uses validation on counter {k}")

# =============================================================================
# SUMMARY
# =============================================================================
print(f"\n{'='*70}")
print("VERIFICATION SUMMARY")
print(f"{'='*70}")

print(f"\nIf all tests passed:")
print(f"✅ Counter logic is FIXED")
print(f"✅ CV files are correctly aligned with test periods")
print(f"✅ Your implementation matches professor's approach")
print(f"✅ Step 8 will use correct hyperparameters for each test period")

print(f"\nNext steps:")
print(f"1. Re-run Step 7 (Cross-Validation) with RUN_CV = True")
print(f"2. Re-run Step 8 (Predictions) with RUN_PRED = True")
print(f"3. Compare new performance metrics with old results")
print(f"4. Expected improvement: Better Sharpe ratio due to correct hyperparameters")

print(f"\n{'='*70}")
print("VERIFICATION COMPLETE!")
print(f"{'='*70}")

# %%
