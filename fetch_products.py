import argparse

import pyrust_exec_engine


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch product data using the Rust extension module."
    )
    parser.add_argument(
        "url",
        nargs="?",
        default="https://fakestoreapi.com/products",
        help="Product API URL to fetch.",
    )
    args = parser.parse_args()

    products = pyrust_exec_engine.fetch_products(args.url)
    print(products)


if __name__ == "__main__":
    main()
