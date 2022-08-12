import t_mcd43gf


def main():

    year_list = ['2018', '2019', '2020', '2021', '2022']

    archive_set = '6'

    t_mcd43gf.create_symbolic_links(year_list, archive_set=archive_set)


if __name__ == '__main__':

    main()