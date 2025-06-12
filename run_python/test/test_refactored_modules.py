# test_refactored_modules.py - Test script for refactored modules
import logging
import sys
import os
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_module_imports():
    """Test that all modules can be imported successfully"""
    print("🔍 Testing module imports...")
    
    try:
        from data_fetcher import DataFetcher
        print("✅ DataFetcher imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import DataFetcher: {e}")
        return False
    
    try:
        from data_transformer import DataTransformer
        print("✅ DataTransformer imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import DataTransformer: {e}")
        return False
    
    try:
        from prefilter import PrefilterManager
        print("✅ PrefilterManager imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import PrefilterManager: {e}")
        return False
    
    try:
        from csv_manager import CSVManager
        print("✅ CSVManager imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import CSVManager: {e}")
        return False
    
    try:
        from workflow_coordinator import WorkflowCoordinator
        print("✅ WorkflowCoordinator imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import WorkflowCoordinator: {e}")
        return False
    
    print("🎉 All modules imported successfully!\n")
    return True

def test_module_initialization():
    """Test that all modules can be initialized"""
    print("🔧 Testing module initialization...")
    
    try:
        from data_fetcher import DataFetcher
        from data_transformer import DataTransformer
        from prefilter import PrefilterManager
        from csv_manager import CSVManager
        from workflow_coordinator import WorkflowCoordinator
        
        # Initialize all modules
        fetcher = DataFetcher()
        print("✅ DataFetcher initialized")
        
        transformer = DataTransformer()
        print("✅ DataTransformer initialized")
        
        prefilter = PrefilterManager()
        print("✅ PrefilterManager initialized")
        
        csv_manager = CSVManager()
        print("✅ CSVManager initialized")
        
        coordinator = WorkflowCoordinator()
        print("✅ WorkflowCoordinator initialized")
        
        print("🎉 All modules initialized successfully!\n")
        return True
        
    except Exception as e:
        print(f"❌ Module initialization failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality of each module"""
    print("⚙️ Testing basic functionality...")
    
    try:
        from data_fetcher import DataFetcher
        from data_transformer import DataTransformer
        from prefilter import PrefilterManager
        from csv_manager import CSVManager
        from workflow_coordinator import WorkflowCoordinator
        
        # Test DataFetcher
        fetcher = DataFetcher()
        # Test method exists
        assert hasattr(fetcher, 'get_symbols_with_fallback'), "DataFetcher missing get_symbols_with_fallback method"
        print("✅ DataFetcher methods check passed")
        
        # Test DataTransformer
        transformer = DataTransformer()
        # Test with sample data
        sample_data = {
            'previous_open': 100.0,
            'previous_high': 105.0,
            'previous_low': 98.0,
            'previous_close': 102.0,
            'previous_volume': 1000000
        }
        is_complete = transformer.is_complete_record(sample_data)
        assert is_complete == True, "DataTransformer should validate complete record"
        print("✅ DataTransformer validation check passed")
        
        # Test PrefilterManager
        prefilter = PrefilterManager()
        # Test with sample data
        sample_stock = {
            'calculated_market_cap': 100.0,  # $100M
            'previous_close': 5.0  # $5.00
        }
        passes_filter = prefilter.apply_prefilter(sample_stock)
        assert passes_filter == True, "Stock should pass prefilter criteria"
        print("✅ PrefilterManager filtering check passed")
        
        # Test CSVManager
        csv_manager = CSVManager()
        fieldnames = csv_manager.get_fieldnames_for_prefiltered_data()
        assert len(fieldnames) > 10, "Should have reasonable number of fieldnames"
        assert 'symbol' in fieldnames, "Should include symbol fieldname"
        print("✅ CSVManager fieldnames check passed")
        
        # Test WorkflowCoordinator
        coordinator = WorkflowCoordinator()
        period_info = coordinator.get_period_info()
        assert 'data_period' in period_info, "Period info should include data_period"
        assert 'current_time_cst' in period_info, "Period info should include current_time_cst"
        print("✅ WorkflowCoordinator period info check passed")
        
        print("🎉 All basic functionality tests passed!\n")
        return True
        
    except Exception as e:
        print(f"❌ Basic functionality test failed: {e}")
        return False

def test_fieldnames_consistency():
    """Test that fieldnames are consistent across modules"""
    print("📋 Testing fieldnames consistency...")
    
    try:
        from csv_manager import CSVManager
        
        csv_manager = CSVManager()
        
        # Get different fieldname sets
        prefiltered_fields = csv_manager.get_fieldnames_for_prefiltered_data()
        regular_fields = csv_manager.get_fieldnames_for_regular_data()
        complete_fields = csv_manager.get_complete_fieldnames_after_8_50()
        
        print(f"  Prefiltered fields: {len(prefiltered_fields)} columns")
        print(f"  Regular fields: {len(regular_fields)} columns")
        print(f"  Complete fields: {len(complete_fields)} columns")
        
        # Validate expected counts
        assert len(prefiltered_fields) == 16, f"Expected 16 prefiltered fields, got {len(prefiltered_fields)}"
        assert len(regular_fields) == 15, f"Expected 15 regular fields, got {len(regular_fields)}"
        assert len(complete_fields) == 30, f"Expected 30 complete fields, got {len(complete_fields)}"
        
        # Check for required fields
        required_basic_fields = ['symbol', 'data_period', 'previous_close', 'calculated_market_cap']
        for fieldset_name, fieldset in [
            ('prefiltered', prefiltered_fields),
            ('regular', regular_fields),
            ('complete', complete_fields)
        ]:
            for field in required_basic_fields:
                assert field in fieldset, f"{fieldset_name} fieldset missing required field: {field}"
        
        # Check for percentage fields in complete set
        expected_percentage_fields = ['today_percentage_8_37', 'today_percentage_8_40', 'today_percentage_8_50']
        for field in expected_percentage_fields:
            assert field in complete_fields, f"Complete fieldset missing percentage field: {field}"
        
        print("✅ Fieldnames consistency check passed")
        print("🎉 All fieldnames are consistent!\n")
        return True
        
    except Exception as e:
        print(f"❌ Fieldnames consistency test failed: {e}")
        return False

def test_configuration_access():
    """Test that modules can access configuration"""
    print("⚙️ Testing configuration access...")
    
    try:
        # Test that config can be imported
        import config
        print("✅ Config module imported")
        
        # Test that key config values exist (even if they're test values)
        required_config = [
            'POLYGON_API_KEY', 'S3_BUCKET', 'SNS_TOPIC_ARN',
            'MIN_MARKET_CAP_MILLIONS', 'MIN_PREVIOUS_CLOSE'
        ]
        
        for config_item in required_config:
            assert hasattr(config, config_item), f"Config missing: {config_item}"
            print(f"✅ Config has {config_item}")
        
        print("🎉 Configuration access test passed!\n")
        return True
        
    except Exception as e:
        print(f"❌ Configuration access test failed: {e}")
        return False

def test_utils_access():
    """Test that modules can access utils"""
    print("🛠️ Testing utils access...")
    
    try:
        # Test that utils can be imported
        import utils
        print("✅ Utils module imported")
        
        # Test that key util functions exist
        required_utils = [
            'get_date_str', 'get_time_str', 'send_sns_notification',
            'upload_to_s3', 'create_stats_counter'
        ]
        
        for util_func in required_utils:
            assert hasattr(utils, util_func), f"Utils missing: {util_func}"
            print(f"✅ Utils has {util_func}")
        
        # Test basic utility functions
        date_str = utils.get_date_str()
        assert len(date_str) == 8, "Date string should be 8 characters (YYYYMMDD)"
        assert date_str.isdigit(), "Date string should be numeric"
        print(f"✅ Date string generation works: {date_str}")
        
        time_str = utils.get_time_str()
        assert len(time_str) == 5, "Time string should be 5 characters (HH:MM)"
        assert ':' in time_str, "Time string should contain colon"
        print(f"✅ Time string generation works: {time_str}")
        
        print("🎉 Utils access test passed!\n")
        return True
        
    except Exception as e:
        print(f"❌ Utils access test failed: {e}")
        return False

def run_comprehensive_test():
    """Run all tests"""
    print("🧪 COMPREHENSIVE MODULE TESTING")
    print("=" * 50)
    print("")
    
    tests = [
        ("Module Imports", test_module_imports),
        ("Module Initialization", test_module_initialization),
        ("Basic Functionality", test_basic_functionality),
        ("Fieldnames Consistency", test_fieldnames_consistency),
        ("Configuration Access", test_configuration_access),
        ("Utils Access", test_utils_access),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"Running: {test_name}")
        print("-" * 30)
        
        try:
            success = test_func()
            if success:
                passed += 1
                print(f"✅ {test_name} PASSED\n")
            else:
                failed += 1
                print(f"❌ {test_name} FAILED\n")
        except Exception as e:
            failed += 1
            print(f"❌ {test_name} FAILED with exception: {e}\n")
    
    # Final summary
    print("=" * 50)
    print("🏁 TESTING SUMMARY")
    print("=" * 50)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Success Rate: {(passed / (passed + failed)) * 100:.1f}%")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! The refactored modules are ready to use.")
        print("\n🚀 Next steps:")
        print("   1. Run a small test: python initial_data_pull.py 10")
        print("   2. Compare output with original system")
        print("   3. Deploy to production environment")
        return True
    else:
        print(f"\n⚠️ {failed} tests failed. Please fix the issues before deployment.")
        print("\n🔧 Troubleshooting:")
        print("   1. Check that all module files are in the same directory")
        print("   2. Verify config.py and utils.py are accessible")
        print("   3. Check Python import path")
        return False

if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1)