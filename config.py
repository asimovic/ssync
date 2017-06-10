import configparser
import os


class ConfigException(Exception):
    pass


class Config(object):
    """
    An object to stick config on.
    """

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            repr_dict_deterministically(self.__dict__),
        )


def repr_dict_deterministically(dict_):
    # a simple version had a disadvantage of outputting dictionary keys in random order.
    # It was hard to read. Therefore we sort items by key.
    fields = ', '.join('%s: %s' % (repr(k), repr(v)) for k, v in sorted(dict_.items()))
    return '{%s}' % (fields,)


def readConfig(filename,
               sectionName,
               required_items,
               optional_items={}):

    def parseItem(item, t, valString):
        if t == bool:
            # bools have to be parsed manually
            if valString == 'True' or valString == 'true':
                return True
            elif valString == 'False' or valString == 'false':
                return False
            else:
                raise ConfigException(
                    f'Config item is not in the correct format: item={item}, expect={t}, val={valString}')
        else:
            # parse all other types
            try:
                return t(valString)
            except ValueError:
                raise ConfigException(
                    f'Config item is not in the correct format: item={item}, expect={t}, val={valString}')

    if not os.path.exists(filename):
        raise FileNotFoundError(f'Config file not found: {filename}')

    config = Config()
    cfg = configparser.ConfigParser()
    cfg.read(filename)
    if not cfg.has_section(sectionName):
        raise ConfigException(f'Invalid Config section not found: [{sectionName}]')
    cSsync = cfg[sectionName]

    for item, t in required_items.items():
        if not cfg.has_option(sectionName, item):
            raise ConfigException(f'Invalid Config item not found: {item}')
        val = parseItem(item, t, cSsync[item])
        setattr(config, item, val)

    for item in optional_items:
        if not cfg.has_option(sectionName, item):
            setattr(config, item, None)
        else:
            val = parseItem(item, t, cSsync[item])
            setattr(config, item, val)

    return config
