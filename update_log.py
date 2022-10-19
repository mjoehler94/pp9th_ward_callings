import time


def main():
    stamp = time.ctime() + "\n"

    with open("log.txt", "a") as f:
        f.write(stamp)
    print(f"Done: {stamp}")


if __name__ == "__main__":
    main()
