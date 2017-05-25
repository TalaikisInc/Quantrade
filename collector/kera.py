from os.path import (join, dirname, isfile)
from six import with_metaclass
from abc import ABCMeta, abstractmethod
from functools import lru_cache

import numpy as np
from pandas import DataFrame, read_pickle, to_datetime
from sklearn import preprocessing
from sklearn.metrics import mean_squared_error, log_loss
from keras.models import Sequential
from keras.layers import recurrent
from keras.layers.core import Dense, Activation, Dropout
from keras.layers.convolutional import Conv1D #, Convolution2D, MaxPooling2D
#from keras.optimizers import SGD , Adam

#from django.conf import settings

#TODO experimntal, not used

def settings():
    s = {'DATA_PATH': dirname(dirname(dirname(__file__)))}
    print(s)
    return s


class Model(with_metaclass(ABCMeta)):
    @abstractmethod
    def fit(self, X, y):
        pass


class Supervised(Model):
    def fit(self, X, y):
        pass


class ANN(Supervised):
    pass


class RNNCell(object):
    RNN  = recurrent.SimpleRNN
    GRU  = recurrent.GRU
    LSTM = recurrent.LSTM


class RNN(ANN):
    def __init__(self, sizes,
                 cell       = RNNCell.LSTM,
                 dropout    = 0.2,
                 activation = 'linear',
                 loss       = 'mse',
                 optimizer  = 'rmsprop'): #beta_1
        self.model = Sequential()

        self.model.add(cell(
            input_dim        = sizes[0],
            output_dim       = sizes[1],
            return_sequences = True
        ))

        for i in range(2, len(sizes) - 1):
            self.model.add(cell(sizes[i], return_sequences = False))
            self.model.add(Dropout(dropout))

        self.model.add(Dense(output_dim = sizes[-1]))
        self.model.add(Activation(activation))

        self.model.compile(loss=loss, optimizer=optimizer)

    def fit(self, X, y, *args, **kwargs):
        return self.model.fit(X, y, *args, **kwargs)

    def predict(self, X):
        return self.model.predict(X)


class SData():
    def __init__(validate, symbol, period):
        self.validate = validate
        self.broker = broker
        self.symbol = symbol
        self.period = period
        self.struc = '{0}_{1}_{2}'.format(self.broker, self.symbol, self.period)
        self.in_path = join(settings.DATA_PATH, 'incoming')
        self.out_path = join(settings.DATA_PATH, 'kera')
        self.fin_filename = join(self.out_path, 'f_{}'.format(self.struc))

    def init_data(self):
        filename = join(self.in_path, "DATA_MODEL_{0}.mp".format(self.struc))
        df = read_csv(filepath_or_buffer=filename, sep=',', delimiter=None, header=0)
        #make index
        df.sort_index(axis=0, ascending=True, inplace=True)
        df.index = to_datetime(df.index).to_pydatetime()
        df.index.name = "DATE_TIME"
        return df

    @lru_cache(maxsize=None)
    def make_targ(self):
        d = self.init_data()
        d['DIFF'] = d['CLOSE'].diff()
        d['TARGET'] = np.where(d['DIFF'].shift() > 0, 1, 0)
        return d['TARGET']

    def make_feat(self):
        d = self.init_data()
        for i in range(1, 100):
            d['dif{}'.format(i)] = d.diff(i)
        for i in range(0, 9):
            d['hc{}'.format(i)] = d.HIGH.shift(i) - d.CLOSE.shift(i)
            d['lc{}'.format(i)] = d.LOW.shift(i) - d.CLOSE.shift(i)
            d['hl{}'.format(i)] = d.HIGH.shift(i) - d.LOW.shift(i)
            d['oc{}'.format(i)] = d.OPEN.shift(i) - d.CLOSE.shift(i)
            d['oh{}'.format(i)] = d.OPEN.shift(i) - d.HIGH.shift(i)
            d['ol{}'.format(i)] = d.OPEN.shift(i) - d.LOW.shift(i)
        d = d.fillna(0)
        d = preprocessing.scale(d)
        filename = join(self.out_poath, 'f_{0}.csv'.format(self.struc))
        d.to_csv(path_or_buf=filename)

    @lru_cache(maxsize=None)
    def masks(self, data):
        train_mask = (self.init_data.index >= 0) & \
            (self.init_data.index <= int(len(self.init_data.index)*0.7))
        test_mask = (self.init_data.index > int(len(self.init_data.index)*0.7)) & \
            (self.init_data.index < len(data.index)-1)
        return (train_mask, test_mask)

    @lru_cache(maxsize=None)
    def data(self):
        if isfile(self.fin_filename):
            df = read_csv(filepath_or_buffer=self.fin_filename, sep=',', delimiter=None, header=0)
            if self.validate:
                df = df.iloc[self.masks()[0]]
            else:
                df = df
        else:
            self.make_feat()
            df = read_csv(filepath_or_buffer=filename, sep=',', delimiter=None, header=0)
            if self.validate:
                df = df.iloc[self.masks()[0]]
            else:
                df = df
        return df

    def train_features(self):
        if self.validate:
            df = self.data().iloc[self.masks()[0]]
        else:
            df = df

    def train_targets(self):
        if self.validate:
            d = self.make_targ().iloc[self.masks()[0]]
        else:
            d = self.make_targ()

    def test_features(self):
        if self.validate:
            df = self.data().iloc[self.masks()[1]]
        else:
            df = df

    def test_targets(self):
        if self.validate:
            d = self.make_targ().iloc[self.masks()[1]]
        else:
            d = self.make_targ()


def main():
    validate = True
    n = SData(validate=validate)

    Xtrain = n.train_features.as_matrix()
    ytrain = n.train_targets
    Xtest = n.test_features.as_matrix()
    ytest = n.test_targets

    Xtrain = np.reshape(Xtrain, (Xtrain.shape[0], Xtrain.shape[1], 1))
    Xtest  = np.reshape(Xtest, (Xtest.shape[0],  Xtest.shape[1], 1))

    rnn = RNN([1, 100, 100, 1])
    rnn.fit(Xtrain, ytrain)
    p = rnn.predict(Xtest)
    p_prob = rnn.predict(Xtest)

    if validate:
        mse = mean_squared_error(ytest, p)
        print("MSE: {}".format(mse))
        loss = log_loss(ytest, p_prob)
        print("Log loss: {}".format(loss))
    else:
        base_path = dirname(__file__)
        results_df = DataFrame(data={'probability':results})
        joined = DataFrame(t_id).join(results_df)
        joined.to_csv(join(base_path, 'results', 'dl.csv'), index=False)
