import pytest

from Pegasus.expressions import InvalidMandatoryClauseError, list_wrapped_range, mandatory_check, mandatory_parse


class TestMandatoryCheck:
    def test_valid_simple_expression(self):
        assert mandatory_check("age > 1") is True

    def test_valid_complex_expression(self):
        assert mandatory_check("age >= 10 and age < 100") is True

    def test_valid_ternary_expression(self):
        assert mandatory_check('"karan" if queue == "test" else "vahi"') is True

    def test_invalid_expression_raises(self):
        with pytest.raises(InvalidMandatoryClauseError) as e:
            mandatory_check("age >< 1")
        assert "Invalid mandatory clause" in str(e.value)

    def test_empty_string_raises(self):
        with pytest.raises(InvalidMandatoryClauseError):
            mandatory_check("")


class TestListWrappedRange:
    def test_single_arg(self):
        assert list_wrapped_range(5) == [0, 1, 2, 3, 4]

    def test_start_stop(self):
        assert list_wrapped_range(2, 6) == [2, 3, 4, 5]

    def test_start_stop_step(self):
        assert list_wrapped_range(0, 10, 2) == [0, 2, 4, 6, 8]

    def test_returns_list(self):
        assert isinstance(list_wrapped_range(3), list)


class TestMandatoryParse:
    def test_simple_comparison(self):
        result = mandatory_parse("dagnode_retry + 1", symbols={"dagnode_retry": 0})
        assert result == 1

    def test_ternary_with_symbols(self):
        result = mandatory_parse(
            '"karan" if queue == "test" else "vahi"', symbols={"queue": "test"}
        )
        assert result == "karan"

    def test_ternary_else_branch(self):
        result = mandatory_parse(
            '"karan" if queue == "test" else "vahi"', symbols={"queue": "prod"}
        )
        assert result == "vahi"

    def test_memory_expr(self):
        result = mandatory_parse(
            "'100MB' if dagnode_retry == 1 else '10MB'",
            symbols={"dagnode_retry": 1},
        )
        assert result == "100MB"

    def test_memory_expr_retry_zero(self):
        result = mandatory_parse(
            "'100MB' if dagnode_retry == 1 else '10MB'",
            symbols={"dagnode_retry": 0},
        )
        assert result == "10MB"

    def test_arithmetic_expression(self):
        result = mandatory_parse("cores * 2", symbols={"cores": 4})
        assert result == 8

    def test_no_symbols(self):
        result = mandatory_parse("1 + 1")
        assert result == 2

    def test_invalid_syntax_raises(self):
        with pytest.raises(InvalidMandatoryClauseError) as e:
            mandatory_parse("age >< 1", symbols={"age": 5})
        assert "Invalid mandatory clause" in str(e.value)

    def test_missing_symbol_raises(self):
        with pytest.raises(InvalidMandatoryClauseError):
            mandatory_parse("age > 1")

    def test_runtime_expr(self):
        result = mandatory_parse(
            "runtime * 2 if dagnode_retry > 0 else runtime",
            symbols={"runtime": 300, "dagnode_retry": 1},
        )
        assert result == 600
