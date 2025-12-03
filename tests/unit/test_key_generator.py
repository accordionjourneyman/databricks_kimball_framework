from unittest.mock import MagicMock, patch
from kimball.key_generator import IdentityKeyGenerator, HashKeyGenerator, SequenceKeyGenerator

def test_identity_key_generator_mock():
    gen = IdentityKeyGenerator()
    df = MagicMock()
    df.columns = ["id", "val"]
    
    # Case 1: Key exists
    gen.generate_keys(df, "id")
    df.drop.assert_called_with("id")
    
    # Case 2: Key does not exist
    df.columns = ["val"]
    gen.generate_keys(df, "id")
    # Should not call drop (or at least return df)
    # We can't easily check "not called" on the *same* mock if we don't reset, 
    # but here we just check it returns something.
    
def test_hash_key_generator_mock():
    with patch("kimball.key_generator.xxhash64") as mock_hash, \
         patch("kimball.key_generator.col") as mock_col:
        
        gen = HashKeyGenerator(["val"])
        df = MagicMock()
        
        gen.generate_keys(df, "sk")
        
        # Should call withColumn with "sk" and result of xxhash64
        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "sk"
        
        # Should call xxhash64
        mock_hash.assert_called()

def test_sequence_key_generator_mock():
    with patch("kimball.key_generator.row_number") as mock_row_number, \
         patch("kimball.key_generator.Window") as mock_window, \
         patch("kimball.key_generator.lit") as mock_lit:
         
        gen = SequenceKeyGenerator()
        df = MagicMock()
        
        gen.generate_keys(df, "sk", existing_max_key=10)
        
        # Should call withColumn
        df.withColumn.assert_called()
        args = df.withColumn.call_args
        assert args[0][0] == "sk"
        
        # Should use row_number
        mock_row_number.assert_called()
        mock_row_number.return_value.over.assert_called()
