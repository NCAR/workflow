try:
    import cPickle as pickle
except:
    import pickle
import argparse

#------------------------------------------------------------
#-- function
#------------------------------------------------------------

def picklepass(kwargs):
    return '"{0}"'.format(pickle.dumps(kwargs))

#------------------------------------------------------------
#-- function
#------------------------------------------------------------

def pickleparse(default={},description='',required_parameters=[]):

    help_str = []
    for k,v in default.items():
        if k in required_parameters:
            help_str.append('%s : REQUIRED'%k)
        elif not v:
            help_str.append('%s : \'\''%k)
        else:
            help_str.append('%s : %s'%(k,v))

    p = argparse.ArgumentParser(description=description)
    p.add_argument('kwargs',
                   default=default,
                   help = '{'+', '.join(help_str)+'}')

    p.add_argument('-f',
                   dest='kwargs_as_file',
                   action='store_true',
                   default=False,
                   help='Interpret input as a file name')

    args = p.parse_args()
    print args.kwargs
    if not args.kwargs_as_file:
        control_in = pickle.loads(args.kwargs)
    else:
        with open(args.kwargs,'r') as fp:
            control_in = pickle.load(fp)

    control = default
    control.update(control_in)

    #-- consider required arguments:
    missing_req = False
    for k in required_parameters:
        if control[k] is None:
            missing_req = True
            print('Missing %s'%k)
    if missing_req:
        raise ValueError('Required args missing: abort.')

    return control
