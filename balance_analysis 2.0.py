# -*- coding: utf-8 -*-
"""
Created on Fri Mar 09 10:08:17 2018

@author: koshnick

"""

import mypy
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from pi_client import pi_client

from pylab import text
from matplotlib.lines import Line2D
from matplotlib.backends.backend_pdf import PdfPages

pi = pi_client()

# =============================================================================
# Constants
# =============================================================================

areas = pd.read_excel('./data/areas.xlsx', index_col=0)
areas = areas.T

chwColor = "#15A9C8"
eleColor = "#98BF47"
steamColor = "#F69321"
gasColor = "#8168AE"
solColor = "#F8D81C"

# =============================================================================
# Functions
# =============================================================================

def data_metics(data):
    """Calculate some statistics about each column in a dataframe over 3 time
    ranges: the last month, the last 6 months, and last 13 months
    """

    stats = data.describe(percentiles=[0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95])

    sections = crop_data(data)

    section1 = data.loc[sections[0], :]
    section2 = data.loc[sections[1], :]
    section3 = data.loc[sections[2], :]

    for col in stats.columns:

        stats.loc['good ratio -13mo', col] = (1
                                              - (len(section1[col])
                                                 - section1[col].count())
                                              / len(section1[col]))

        stats.loc['good ratio -12-2mo', col] = (1
                                                - (len(section2[col])
                                                    - section2[col].count())
                                                / len(section2[col]))

        stats.loc['good ratio -1mo', col] = (1
                                             - (len(section3[col])
                                                 - section3[col].count())
                                             / len(section3[col]))

    return stats.T


def get_pi_data(end):
    """
    Get all of the pi data for the buildings kbtu tags. Only get tags for bldgs
    that have all 3 tags in PI

    """

    tags = pi.search_by_point(['*_Electricity_Demand_kBtu',
                               '*_ChilledWater_Demand_kBtu',
                               '*_Steam_Demand_kBtu'])

    tags = split_tags(tags)

    tags = pi.group_tags(tags,
                         parentLevel=1,
                         sensorGroup=['Electricity_Demand_kBtu',
                                      'ChilledWater_Demand_kBtu',
                                      'Steam_Demand_kBtu'],
                         sep='@')

    tags = [tag.replace('@','_') for tag in tags]

    print(len(tags))

#    tags = tags[0:6]

    df = pi.get_stream_by_point(tags, start='2017-01-01', end=end,
                                interval='1h', calculation='summary',
                                _chunk_size=20, _buffer_time=0)

    return df

    # get all tags with kbtu demand
    # filter based on endings
    # pi_client group function


def split_tags(tags):
    """ Helper function to remove the kBtu tags from building name """
    endings = ['_Electricity_Demand_kBtu',
               '_ChilledWater_Demand_kBtu',
               '_Steam_Demand_kBtu']

    for i, tag in enumerate(tags):
        firstIndex = -1
        for ending in endings:
            firstIndex = tag.find(ending)
            if firstIndex > 0:
                # Replace the seperator of bldg and tag with @ (bldg@tag)
                # For easy splitting of the names later. @ never shows itself
                tags[i] = tag[0:firstIndex] + '@' + tag[firstIndex+1:len(tag)]
                break

    return tags


def energy_balance(data, period='daily'):
    """ Calculates the steam + elec - chilled water value known as the energy
    balance then divides it by the square footage of the building and converts
    to kBtu. Ooutputs the the usage data, and balance data at the resampled
    rate specified by period="""

    # Constants
    chw, steam, ele = ('ChilledWater_Demand_kBtu',
                       'Steam_Demand_kBtu',
                       'Electricity_Demand_kBtu')

    data.columns = split_tags(list(data.columns))

#    data = data[data.columns[102:153]].copy() * 1
    data = data.copy() * 1000

    # Build columns
    data = mypy.merge_oat(data)

    # Columns should all be aggregated by SUM, except OAT should be mean
    aggDict = {}
    for column in data.columns:
        aggDict[column] = 'sum'
    aggDict['OAT'] = 'mean'

    skipList = ['year', 'month', 'day', 'hour', 'minute', 'weekday', 'daytime',
                'saturday', 'sunday', 'OAT', 'daysinmonth', 'days']

    # Grouping
    if period == 'daily':

        grouped = data.resample('D').agg(aggDict)
        oat = grouped['OAT']

    elif period == 'monthly':

        grouped = data.resample('M').agg(aggDict)
        grouped['days'] = grouped.index.daysinmonth
        oat = grouped['OAT']
        # Ensure that monthly use is sacled by days in month
        grouped = grouped.divide(grouped['days'], axis=0)

    grouped.columns = mypy.make_multi_index(grouped.columns, splitString='@')
    buildings = grouped.columns.get_level_values(0).unique()

    balanceOutput = pd.DataFrame()


    for build in buildings:

        print(build)

        if build in skipList:
            print('skipping ', build)
            continue
        try:
            area = areas[build]
        except KeyError:
            print('{} not in areas document'.format(build))
            continue

        for comm in [chw, steam, ele]:
            # For each commodity. divide by area of that comm
            # set the area normalized values back into the grouped df
            grouped.loc[:,(build, comm)] = (grouped[build][comm]/(area[comm])).values

        try:
            # Should already be area normalized
            balanceOutput[build] = (grouped[build][steam]
                                    + grouped[build][ele]
                                    - grouped[build][chw])
        except KeyError:
            print('some commodity missing from {}'.format(build))
            print('columns availible are {}'.format(demands.columns))
            continue

    # Append oat back to dfs (oat was properly resampled above)
    grouped['OAT'] = oat
    balanceOutput['OAT'] = oat

    return balanceOutput, grouped


def crop_data(data, endDate='2018-09-01'):
    """ Enter the start of the following month to recieve the 1,11,1 month
    date sections to use for plotting/slicing """

    splitDate = endDate.split('-')
    y = int(splitDate[0])
    m = int(splitDate[1])
    d = splitDate[2]

    index0 = pd.to_datetime('-'.join([str(y-1), str(m-1), d]))
    index1 = pd.to_datetime('-'.join([str(y-1), str(m), d]))
    index2 = pd.to_datetime('-'.join([str(y), str(m-1), d]))
    index3 = pd.to_datetime('-'.join([str(y), str(m), d]))

    section1 = data[index0:index1]
    section2 = data[index1:index2]
    section3 = data[index2:index3]

    return section1, section2, section3


def use_plotting(data,
                 ax,
                 name=None,
                 period='Daily',
                 time=False,
                 endDate=None):
    """ Generates the time series plot, or the use vs OAT plot """

    years = data.index.year.unique()

    # if monthly make scatter plot of all years
    # try to shade by years
    # try to get 1 year of previous data
    #  make a line plot of use
    # shade old and new
    # put OAT with line plot on secondary axis
    # add grid lines

    sections = crop_data(data, endDate=endDate)

    data = pd.concat(sections)
    data = mypy.build_time_columns(data)

    years = data.index.year.unique()
    demandTags = ['Electricity_Demand_kBtu',
                  'ChilledWater_Demand_kBtu',
                  'Steam_Demand_kBtu']

    colors = [eleColor, chwColor, steamColor]

    # Set up plot axes
    for i, tag in enumerate(demandTags):
        # Iterate through commodities: chw steam ele
        if period == 'Daily':
            if time:
                # Grab multiindex Building and tag
                # then slice down tdat time series axis
                for j, section in enumerate(sections):
                    if j == 0 or j == 2:
                        width = 1.2
                    else:
                        width = 0.3
                    try:
                        ydat = section[name][tag]
                    except KeyError:
                        print('commodity missing from {}'
                              ' in use_plotting'.format(name))
                        continue
                    myFmt = mdates.DateFormatter('%m-%y')
                    ax.xaxis.set_major_formatter(myFmt)

                    ax.plot(section.index, ydat,
                            color=colors[i],
                            lineWidth=width)

            else:  # time == False
                numYears = len(years)
                alphaDelta, alphaValue = 1.0 / numYears+.1, -.2

                for j, year in enumerate(years):
                    # Make alpha values [n1, ..., nN]
                    alphaValue += alphaDelta
                    try:
                        ydat = data[data['year'] == year][name][tag]
                    except KeyError:
                        print('commodity missing from {}'
                              ' in use_plotting'.format(name))
                        continue
                    xdat = data[data['year'] == year]['OAT']

                    ax.scatter(xdat, ydat,
                               color=colors[i],
                               label=tag,
                               alpha=alphaValue,
                               s=10)

                    legendElements = [Line2D([0], [0], lw=0, color=eleColor,
                                             label='ELE', marker='o'),
                                      Line2D([0], [0], lw=0, color=chwColor,
                                             label='CHW', marker='o'),
                                      Line2D([0], [0], lw=0, color=steamColor,
                                             label='STM', marker='o')]

                    ax.legend(handles=legendElements,
                              loc='upper center', bbox_to_anchor=(0.5, 0.99),
                              ncol=3, fancybox=True, shadow=True)

        if period == 'Monthly':
            ax.scatter(xdat, ydat,
                       color=colors[i],
                       label=tag)

        # AXIS PARAMS
        if time:
            xLabel = 'Date'
        else:
            xLabel = 'OAT   [$^\circ$F]'

        if time:
            titleString = '{} Demand Past 13 Months'.format(name)
        else:
            titleString = '{} Demand vs OAT'.format(name)

        if not time:
            ax.set_xlim(xLim)
        ax.tick_params(axis='both', labelsize=textSize)
        ax.set_title(titleString, fontSize=headingSize)
        ax.set_xlabel(xLabel, fontSize=headingSize)
        ax.set_ylabel('Demand   [BTU / day/ ft$^2$]', fontSize=headingSize)
        # END PARAMS


def slope_calc(x, y):
    """ Calcs the slope and x-intercept for balance plotting using last 13M """

    newStart = x.index[-1] + pd.offsets.DateOffset(months=-13)

    x = x[newStart:]
    y = y[newStart:]

    idx = np.isfinite(x) & np.isfinite(y)
    try:
        fitValues = np.polyfit(x[idx], y[idx], 1)
        m, b = fitValues[0], fitValues[1]
        x_0 = -b / m
    except:
        m, b, x_0 = 10, 10, 10

    return m, b, x_0


def balance_plotting(data, ax, period='Daily'):
    """ Plot the energy balance data as either "Daily" or "Monthly" """

    data.columns = split_tags(list(data.columns))

    build = data.columns[0]
    data = mypy.build_time_columns(data)
    years = data.index.year.unique()
    colors = sns.hls_palette(4, l=.4, s=.9)[2:4]  # Radical 90s water cup color

    x = data['OAT']
    y = data[build]

    m, b, x_0 = slope_calc(x, y)

    text(0.80, 0.84, 'X-int: {}'.format(round(x_0)),
         ha='left', va='center', transform=ax.transAxes)
    text(0.80, 0.80, 'Slope: {}'.format(round(m, 1)),
         ha='left', va='center', transform=ax.transAxes)

    for i, year in enumerate(years):

        xdat = data[data['year'] == year]['OAT']
        ydat = data[data['year'] == year][build]

        if period == 'Monthly':
            ax.plot(xdat, ydat,
                    color=colors[i],
                    marker='o',
                    linestyle='-',
                    label=year,
                    markersize=5,
                    linewidth=1)

            ax.plot(xdat[0], ydat[0],
                    color=colors[i],
                    marker='*',
                    linestyle=' ',
                    markersize=15)

        if period == 'Daily':
            ax.plot(xdat, ydat,
                    marker='o',
                    color=colors[i],
                    linestyle=' ',
                    label=year,
                    markersize=4)

        # AXIS PARAMS
        ax.set_xlim(xLim)
        ax.axvline(x=x_0, color='xkcd:steel grey', linewidth=1)
        ax.axhline(y=0, color='k', linewidth=1)
        ax.set_xlabel('OAT   [$^\circ$F]', fontSize=headingSize)
        ax.set_ylabel('Energy Balance   [BTU / day / ft$^2$]',
                      fontSize=headingSize)

        titleString = '{} {} Energy Balance'.format(build, period)

        ax.set_title(titleString, fontSize=headingSize)
        ax.tick_params(axis='both', labelsize=textSize)
        # END PARAMS

    ax.legend(loc='upper right', fontsize=textSize)


def quad_plot(monthBalance, monthUse, dayBalance, dayUse, endDate=None):
    """ Combines the 4 plots of interest for the energy balance analysis into
    a single plot. does this for each building and saves the entire thing in
    a single PDF file """

    buildings = monthBalance.columns
    # make PDF writer with name of the month after the month being analyzed
    pdfWriter = PdfPages("Energy Balance {}.pdf".format(endDate))

    # Iterate over building
    for _, build in enumerate(buildings):

        if _ > 300:
            break

        print('{} of {}: {}'.format(_, len(buildings), build))

        if build == 'OAT':
            print('Skipping OAT')
            continue

        # Make 1  of each plot - have the plot functions draw on the ax object
        # Put all of those ax objects into a single plot
        f, axes = plt.subplots(2, 2,
#                               sharey=True,  #TODO: get share y on top
                               figsize=(figureSize[0]*2, figureSize[1]*2))

        balance_plotting(monthBalance[[build, 'OAT']], ax=axes[0, 0],
                         period="Monthly")
        balance_plotting(dayBalance[[build, 'OAT']], ax=axes[0, 1],
                         period="Daily")

        use_plotting(dayUse[[build, 'OAT']], ax=axes[1, 0], time=True,
                     name=build, endDate=endDate)

        use_plotting(dayUse[[build, 'OAT']], ax=axes[1, 1], time=False,
                     name=build, endDate=endDate)

        # PDF save that plot
        plt.savefig(pdfWriter, format='pdf', bbox_inches='tight')
        plt.close()

    pdfWriter.close()

# Plot global parameters
xLim = (35, 99)  # OAT x-axis limits (drop 100 for cleaner x axis)
figureSize = (8, 8)  # size of individial plot, larger plot is 2x bigger
textSize = 14  # Text size for tick marks etc
headingSize = 16 # text size for plot title and axes titles
sns.set()

if __name__ == "__main__":

#    data = get_pi_data(end = dateString)
    dateString = '2018-10-30'

    # Gross outlier remove technique
    lowerBound = -0.1
    upperBound = 1000000
    data = data[(data > lowerBound) & (data < upperBound)]

    # % Run balances
    monthBalance, monthUse = energy_balance(data, period='monthly')
    dayBalance, dayUse = energy_balance(data, period='daily')

    # % Plotting
    quad_plot(monthBalance, monthUse, dayBalance, dayUse, endDate=dateString)

