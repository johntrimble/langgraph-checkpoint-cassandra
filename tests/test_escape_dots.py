from hypothesis import given, strategies as st

from langgraph_checkpoint_cassandra.cassandra_saver import _escape_dots, _unescape_dots


@given(
    st.builds(
        "".join,
        st.lists(
            st.one_of(
                st.just("."),
                st.just("\\"),
                st.sampled_from(["a", "b", "c", "d", "e", "f", "g", "h"]),
            ),
            max_size=50,
        ),
    )
)
def test_escape_round_trip(string: str) -> None:
    escaped = _escape_dots(string)
    unescaped = _unescape_dots(escaped)
    assert unescaped == string
