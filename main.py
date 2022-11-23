import sys


def main():
    file_name = sys.argv[0]
    latest_year = str(sys.argv[1])
    latest_month = str(sys.argv[2])

    if latest_year != None & latest_month != None:
        print("test output latest_year:{}  latest_month:{}".format(
            latest_year, latest_month))
    else:
        print("please input latest_year and latest_month")


if __name__ == '__main__':
    main()
