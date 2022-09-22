import argparse
import t_mcd43gf

if __name__ == '__main__':

    # Make an argument parser instance
    parser = argparse.ArgumentParser()
    # Add an argument for the years
    parser.add_argument('-y',
                        '--years',
                        nargs='+',
                        help='Enter years (space-separated)',
                        required=True)
    # Parse any argument
    args = parser.parse_args()
    # Get input data for years
    t_mcd43gf.get_input_data_for_gapfilled(args.years, archive_set=61)
    # Create the symbolic links
    t_mcd43gf.create_symbolic_links(args.years, archive_set=61)
