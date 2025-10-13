import json
import re
import matplotlib.pyplot as plt



def read_json(filepath: str) -> dict:
    try:
        with open(filepath, 'r') as json_file:
            result = json.load(json_file)
            return result
    except Exception as err:
        print("Error while reading|parsing the json file", filepath)
        raise err

def write_json(filepath: str, json_output: dict | list) -> bool:
    try:
        with open(filepath, 'w') as json_file:
            json.dump(json_output, json_file)
    except Exception as err:
        print(f"[ERROR] While writing json result to {filepath}: ", err)
        return False
    return True

def normalize_result(result: list) -> tuple:
    return tuple(map(
        lambda x: sorted(x, key= hash),
        result
    ))

# NOTE it doesn't check for order 
def check_equality(table1: list, table2: list) -> bool:
    for a, b in zip(normalize_result(table1), normalize_result(table2)):
            if a != b:
                return False
    
    return True

def json_stringify(json_obj: dict | list) -> str:
    return json.dumps(json_obj)


def human_in_the_loop(msg: str, confirm_input: str = 'y', cancel_input: str = 'n') -> bool:
    user_input = cancel_input
    while user_input.lower() != confirm_input:
        user_input = input(f"{msg}")
        if user_input == cancel_input:
            return False
    return True


# GEMINI imported
def remove_limit_clause(sql_query: str) -> str:
    """
    Removes the "LIMIT <number>" clause from an SQL query string.

    This function uses a regular expression to handle variations in:
    1. Case (e.g., 'LIMIT', 'limit', 'Limit').
    2. Spacing around the LIMIT keyword and the number.
    3. An optional trailing semicolon.

    Args:
        sql_query: The input SQL query string.

    Returns:
        A copy of the SQL query string with the LIMIT clause removed.
    """
    # Pattern to match 'LIMIT <number>' at the very end of the string.
    # Components:
    # 1. (\s+): Match one or more whitespace characters (to capture the space
    #    before LIMIT and ensure cleanliness when removing).
    # 2. (LIMIT\s+\d+): Match "LIMIT", followed by one or more spaces,
    #    followed by one or more digits (the limit value).
    # 3. (\s*;\s*)?: Match optional trailing space, optional semicolon,
    #    and optional trailing space.
    # 4. ($): Anchor to the end of the string.
    # re.IGNORECASE makes the matching case-insensitive.
    limit_pattern = r'\s+LIMIT\s+\d+\s*;?\s*$'
    
    # Use re.sub to replace the matched pattern with an empty string ('')
    cleaned_query = re.sub(limit_pattern, '', sql_query, flags=re.IGNORECASE)
    
    return cleaned_query.strip() # .strip() ensures no leading/trailing space is left


def create_graph(output_path: str, categories: list[str], values: list[int], xlabel:str, ylabel:str, title: str, figsize: tuple = (8, 5), color='skyblue'):
        plt.figure(figsize=figsize)
        plt.bar(categories, values, color=color)

        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)

        plt.grid(axis='y', linestyle='--', alpha=0.7) # Add horizontal grid lines
        plt.tight_layout()
        plt.savefig(output_path)
if __name__ == "__main__":
    # --- TEST ---

    query1 = "SELECT * FROM Users WHERE Status = 'Active' ORDER BY CreatedDate DESC LIMIT 20;"
    query2 = "select first_name, last_name from customers where country <> 'USA' limit 10"
    query3 = "SELECT CustomerId, FullName FROM Customer limit 5;"
    query4 = "SELECT ProductID, Name FROM Products;" # No limit clause

    print("Original Query 1:")
    print(query1)
    print("Cleaned Query 1:")
    print(remove_limit_clause(query1))
    print("-" * 20)

    print("Original Query 2:")
    print(query2)
    print("Cleaned Query 2:")
    print(remove_limit_clause(query2))
    print("-" * 20)

    print("Original Query 3 (with semicolon):")
    print(query3)
    print("Cleaned Query 3:")
    print(remove_limit_clause(query3))
    print("-" * 20)

    print("Original Query 4 (no limit):")
    print(query4)
    print("Cleaned Query 4:")
    print(remove_limit_clause(query4))
