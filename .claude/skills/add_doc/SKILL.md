description: Add Explanation or Google-style docstrings to functions and classes in the target file
allowed-tools: Read, Edit

Add Explanation or Google-style docstrings to functions and classes in $ARGUMENTS. 

When there is a keyword 'TODO: ADD_EXPLAIN' then add one-line or max two-lines explanation for the given attribute or the line of the code where keyword TODO: ADD_EXPLAIN is present. ADD_EXPLAIN keyword may be followed by some context, so use that when adding explanation. Preserve any existing explanation, only improve them. 

When there is a keyword 'TODO: ADD_DOC', add the documentation only on that function.

Documentation should have following
1. One-line summary
2. Args: name, type, description
3. Returns: type and description
4. Raises: any exceptions

Preserve existing docstrings, only improve them.