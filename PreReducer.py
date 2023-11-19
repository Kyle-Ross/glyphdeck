# Moved this to https://github.com/Kyle-Ross/elastic-llm-nlp - 2023-11-19

import pandas as pd
from typing import Union, Optional, List, Dict, Any


class PreReducer:
    """Takes a datasource and reduces it to desired columns in a consistent format
    in preparation for further processing which relies on that consistent format.
    Returns the id column as 'id' and the text columns as 'text1', 'text2' etc"""
    def __init__(self,
                 id_col: str,
                 text_col: Union[str, List],
                 rename=True) -> None:
        self.id_col: str = id_col
        self.text_col: Union[str, List] = text_col
        self.rename: bool = rename
        self.raw_input: Optional[pd.DataFrame] = None
        self.reduced_input: Optional[List[Dict[str, Any]]] = None
        self.keep_cols: Optional[List] = None
        self.new_cols: Optional[List] = None
        self.name_ref: Optional[Dict] = None
        self.name_status: str = 'original'

        # If text col is a single item list, convert it to a string
        if isinstance(text_col, list) and len(text_col) == 1:
            self.text_col: Union[str, List] = str(text_col[0])

    def reduce(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Filters a dataframe and renames columns the same way for any dataframe.
        Renaming is optional but on by default."""
        # Defining the basis of the new column names
        id_col_name = 'id'
        text_col_prefix = 'text'
        # Saving conditionals for use below
        is_str = isinstance(self.text_col, str)
        is_list = isinstance(self.text_col, List)
        list_len = len(self.text_col) if is_list else None
        # Create the list of columns to keep and their new names based on type
        if is_str or (is_list and list_len == 1):
            self.keep_cols = [self.id_col, self.text_col]
            self.new_cols = [id_col_name, text_col_prefix + str(1)]
        elif is_list:
            self.keep_cols = [self.id_col] + self.text_col
            self.new_cols = [id_col_name] + [text_col_prefix + str(x) for x in range(1, list_len + 1)]
        else:
            raise TypeError("text_col must be a string or list")
        # Store a dict of the new and old names i.e. {'id': 'UserName', 'text1': 'OriginalTweet}
        self.name_ref = dict(zip(self.new_cols, self.keep_cols))
        # Subset & rename the data
        subset = df[self.keep_cols]
        if self.rename:
            self.name_status = 'new'
            subset.columns = self.new_cols
        # Rename the columns unless the setting is turned off
        as_dict = subset.to_dict('records')
        return as_dict

    def original_names(self):
        """For each pair in the list of dicts,
        replace the key with the value from the matching pair in the reference"""
        if self.name_status == 'new':
            self.reduced_input = [{self.name_ref.get(k): v for k, v in d.items()} for d in self.reduced_input]
            return self

    def new_names(self):
        """Create a reversed dictionary and swap old names to the new"""
        if self.name_status == 'original':
            reversed_name_ref = {v: k for k, v in self.name_ref.items()}
            self.reduced_input = [{reversed_name_ref.get(k): v for k, v in d.items()} for d in self.reduced_input]
            return self

    def csv(self, path: str, encoding: str = "utf-8") -> 'PreReducer':
        """Processing method for when the input is a csv"""
        self.raw_input = pd.read_csv(path, encoding=encoding)
        self.reduced_input = self.reduce(self.raw_input)
        return self

    def xlsx(self, path: str) -> 'PreReducer':
        """Processing method for when the input is a xlsx"""
        self.raw_input = pd.read_excel(path)
        self.reduced_input = self.reduce(self.raw_input)
        return self


if __name__ == "__main__":
    """Only runs below if script is run directly, not on import, so this is for testing purposes"""
    # Set the file path
    input_file_path = "../datasets/Kaggle - Coronavirus tweets NLP - Text Classification/Corona_NLP_train.csv"
    # Create the object, with id in first arg and 1 or more columns in the second
    # Use the csv method to get the data and run everything else in the background
    # Can pass a string or list to the second arg
    reduction_obj = PreReducer("UserName", ["OriginalTweet", "Location"]).csv(input_file_path, "ISO-8859-1")
    # Arguments you can use to swap the field names back and forth to new and old as you like
    # input_data_obj.original_names()
    # input_data_obj.new_names()
    # The final output which is a list of dictionaries
    print(reduction_obj.reduced_input)

# TODO
# add comment sanitiser logic in here? or connected in a diff class.
# it should at least be inherited here. After that it might be good to rename it as PreProcessor
# Thing about any other preprocessing steps there might be if any, like multiple columns or anything like that
# Or maybe any other modules should be built specifically to run off this structured output
# Option to combine or concatenate multiple comments
