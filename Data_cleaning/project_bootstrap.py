# Data_Cleaning/project_bootstrap.py

import os
import sys

def bootstrap_project_paths(current_file: str, anchor_dir: str = "src", max_depth: int = 6):
    """
    Recursively walks up the folder tree to find a directory containing 'src'.
    Once found, it adds that path to sys.path so 'src' becomes importable from anywhere.
    
    Arguments:
        current_file: Usually __file__, representing the script calling this function.
        anchor_dir: The folder name to look for (default is 'src').
        max_depth: How many parent levels to go up (default is 6).

    Raises:
        FileNotFoundError if the anchor folder isn't found in the hierarchy.
    """
    current_dir = os.path.abspath(os.path.dirname(current_file))

    for _ in range(max_depth):
        possible_path = os.path.join(current_dir, anchor_dir)
        if os.path.isdir(possible_path):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
                print(f"✅ Bootstrapped project path: {current_dir}")

            # Load .env from project root
            from dotenv import load_dotenv
            env_path = os.path.join(current_dir, ".env")
            if os.path.exists(env_path):
                load_dotenv(env_path)
                print(f"✅ .env loaded from: {env_path}")
            else:
                print(f"⚠️  No .env file found at: {env_path}")
            
            return
        current_dir = os.path.dirname(current_dir)

    raise FileNotFoundError(f"❌ Could not find folder '{anchor_dir}' within {max_depth} levels up from {current_file}")
