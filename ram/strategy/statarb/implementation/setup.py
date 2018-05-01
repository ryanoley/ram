import os
from ram import config


def main():
    check_implementation_folder_structure()


def check_implementation_folder_structure():
    """
    Folder structure is created according to how the system expects
    it to function.
    """
    ddir = config.IMPLEMENTATION_DATA_DIR

    path = os.path.join(ddir, 'StatArbStrategy')
    _check_create(path)

    path2 = os.path.join(path, 'live')
    _check_create(path2)

    # Archive has many different file types associated with it
    path2 = os.path.join(path, 'archive')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'version_data')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'ticker_mapping')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'qad_scaling')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'bloomberg_scaling')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'live_pricing')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'size_containers')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'allocations')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'pretrade_checks')
    _check_create(path2)

    path2 = os.path.join(path, 'archive', 'live_directories')
    _check_create(path2)

    return


def _check_create(path):
    if not os.path.isdir(path):
        os.mkdir(path)
    return


if __name__ == '__main__':
    main()
