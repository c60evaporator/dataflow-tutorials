CK = ''
CS = ''
AT = ''
AS = ''

import tweest

def test(status):
    print(status)

tweest.set_auth(CK,CS,AT,AS)
tweest.start({'track':'あ'},test)