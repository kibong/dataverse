
import os
import datasets
from pathlib import Path
from omegaconf import ListConfig


def load_huggingface_dataset(name_or_path, split=None):
    """
    load huggingface dataset
    
    Args:
        name_or_path (str or list): the name or path of the huggingface dataset
        split (str): the split of the dataset
    """
    # load huggingface dataset
    if isinstance(name_or_path, str):
        dataset = datasets.load_dataset(name_or_path, split=split)
    elif isinstance(name_or_path, list):
        dataset = datasets.load_dataset(*name_or_path, split=split)
    elif isinstance(name_or_path, ListConfig):
        dataset = datasets.load_dataset(*name_or_path, split=split)
    else:
        raise ValueError(f"Unsupported type of name_or_path: {type(name_or_path)}")

    return dataset

def huggingface2parquet(
        dataset: datasets.Dataset,
        cache_path: str = None,
        verbose: bool = True,
        **kwargs
    ):
    """
    Convert a huggingface dataset to parquet format and save it to the path.
    
    Args:
        dataset (datasets.Dataset): a huggingface dataset
        cache_path (str): cache path to save the dataset
        verbose (bool): whether to print the information of the dataset
    """
    # check the dataset which has train, test, validation or other splits
    # concatenate all the splits into one
    dataset_list = []

    # check the dataset has splits
    try:
        for split in dataset.keys():
            dataset_list.append(dataset[split])
    except:
        dataset_list.append(dataset)

    dataset = datasets.concatenate_datasets(dataset_list)

    # save the dataset to parquet
    # FIXME: this is a temporary solution to store the dataset in the package root path
    #        we will change it to a better solution in the future
    if cache_path is None:
        # save the parquet at package root path
        cache_path = Path(os.path.abspath(__file__)).parents[3]

    dataset_path = f"{cache_path}/.cache/dataverse/dataset/huggingface_{dataset._fingerprint}.parquet"

    # check the dataset exist
    if os.path.exists(dataset_path):
        if verbose:
            print(f"Dataset already exists at {dataset_path}")
        return dataset_path

    os.makedirs(f"{cache_path}/.cache/dataverse/dataset", exist_ok=True)
    dataset.to_parquet(dataset_path)

    return dataset_path

if __name__ == "__main__":
    # test the function
    dataset = load_huggingface_dataset(["glue", "mrpc"])
    dataset_path = huggingface2parquet(dataset, verbose=True)

    print(f"Dataset saved at {dataset_path}")