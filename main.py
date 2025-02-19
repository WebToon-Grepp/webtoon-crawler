import argparse
import naver.fetcher
import kakao.fetcher

def main(target):
    if target == "naver":
        naver.fetcher.fetch_all_data()
    elif target == "kakao":
        kakao.fetcher.fetch_all_data()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch webtoon data")
    parser.add_argument(
        "-t", "--target",
        type=str,
        choices=["naver", "kakao"],
        required=True,
        help="Select the webtoon platform to fetch data from: 'naver' or 'kakao'",
    )
    args = parser.parse_args()
    main(args.target)
