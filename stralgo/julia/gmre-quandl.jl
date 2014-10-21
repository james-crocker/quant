Pkg.add("Quandl")
Pkg.add("StatsBase")
using Quandl
using StatsBase

DATE_END = Date("2014-10-20")

function rs_volatility(history::TimeArray{Float64, 2})

    r = 0

#println(string("History Length ",length(history)))

    for bar in history

        open = bar[2][1]
        high = bar[2][2]
        low = bar[2][3]        
        close = bar[2][4]

        a = log(high / close)
        b = log(high / open)
        c = log(low / close)
        d = log(low / open)

        r += ((a * b) + (c * d))

    end

    x = sqrt(r / length(history))
    #println(string("VOL ", x))
    return x

end

function volatility(compute::String, months::Int, window::Int, basket::Dict{Any, Any})

    basket_volatilities = Dict()

    # TimeArray date order is ascending slice d1<d2 : d2>d1 eg 2014-07-07:2014-08-22
    date_end = DATE_END
    date_begin = date_end - Dates.Month(months)

    if lowercase(compute) == "sliding"
        for stock in keys(basket)
            vol = Float64[]
            date_slice_end = date_end
            date_slice_begin = date_end - Dates.Day(window)
            while date_slice_begin > date_begin
                push!(vol, rs_volatility(basket[stock][[date_slice_begin:date_slice_end]]))
                date_slice_begin -= Dates.Day(1)
                date_slice_end -= Dates.Day(1)
            end
            basket_volatilities[stock] = mean(vol)
        end
    elseif lowercase(compute) == "grouping"
       for stock in keys(basket)
            vol = Float64[]
            date_slice_end = date_end
            date_slice_begin = date_end - Dates.Day(window)
#println(string("Grouping Date End ", date_slice_end, " Date Begin ", date_slice_begin))
            while date_slice_begin > date_begin
                push!(vol, rs_volatility(basket[stock][[date_slice_begin:date_slice_end]]))
                date_slice_begin -= Dates.Day(window)
                date_slice_end -= Dates.Day(window)
            end
            basket_volatilities[stock] = mean(vol)
        end
    elseif lowercase(compute) == "all"
       for stock in keys(basket)
            vol = Float64[]
            date_slice_end = date_end
            date_slice_begin = date_end - Dates.Month(months)
#println(string("Grouping Date End ", date_slice_end, " Date Begin ", date_slice_begin))
            push!(vol, rs_volatility(basket[stock][[date_slice_begin:date_slice_end]]))
            basket_volatilities[stock] = mean(vol)
        end
    end

    return basket_volatilities

end

function performance(compute::String, months::Int, basket::Dict{Any, Any})

    basket_performances = Dict()

    for stock in keys(basket)
        bsize = length(basket[stock])
        
        # TimeArray date order is ascending so slice is earliest date(end) :to: latest date(begin)
        # Date slicing in the timearray will capture elements in the range even if
        # no exact date match. 2014-07-05:2014-08-23 returns 2014-07-07:2014-08-21

        date_end = DATE_END
        date_begin = date_end - Dates.Month(months)

        prices = basket[stock]["Open", "Close"][[date_begin:date_end]]  

        price_begin = 0
        price_end = 0

        if lowercase(compute) == "open"
            price_begin = prices[1].values[1]
            price_end = prices[length(prices)].values[1]
        elseif lowercase(compute) == "close"
            price_begin = prices[1].values[2]
            price_end = prices[length(prices)].values[2]
        elseif lowercase(compute) == "openclose"
            price_begin = prices[1].values[1]
            price_end = prices[length(prices)].values[2]
        elseif lowercase(compute) == "closeopen"
            price_begin = prices[1].values[2]
            price_end = prices[length(prices)].values[1]
        end

        basket_performances[stock] = (price_end - price_begin) / price_begin

    end

    return basket_performances

end

function minmax(basket::Dict{Any, Any})
    return (maximum(Base.values(basket)), minimum(Base.values(basket)))
end

function rank_basket(volatility::Dict{Any, Any}, performance::Dict{Any, Any})

    minp, maxp = minmax(performance)
    minv, maxv = minmax(volatility)

    pfactor = 0.7
    vfactor = 0.3
    ranks = Dict()

    for stock in keys(basket)
        p = (performance[stock] - minp) / (maxp - minp)
        #v = (1 - (volatility[stock] - minv)) / (maxv - minv)
        v = (volatility[stock] - minv) / (maxv - minv)

        if lowercase(stock) == "edv"
             ranks[stock] = (p * pfactor) + ((v * 0.5) * vfactor)
             #ranks[stock] = (p * pfactor) + (v * vfactor)
        else
             ranks[stock] = (p * pfactor) + (v * vfactor)
        end
    end

    minr, maxr = minmax(ranks)
    basket_rank = Array((String, Float64), length(ranks))
    n = 1
    for stock in keys(ranks)
        basket_rank[n] = (stock, (ranks[stock] - minr) / (maxr - minr))
        n += 1
    end

    return sort(basket_rank, by=x->x[2], rev=true)

end

function raw_basket(volatility::Dict{Any, Any}, performance::Dict{Any, Any})

    raw = Dict()

    for stock in keys(basket)
        p = performance[stock]
        v = 1 - volatility[stock]
        #v = (volatility[stock] - minv) / (maxv - minv)
        raw[stock] = (v, p)
#println(string(p, " ", v))
    end

    return raw

end

#======================================================#
#Pkg.add("Datetime")
#Pkg.add("TimeSeries")
#Pkg.add("Quandl")

set_auth_token("put_in_your_quandl_token")

quandl_basket = [
"EPP" => "GOOG/NYSE",
"ILF" => "GOOG/AMEX",
"EEM" => "GOOG/NYSE",
"IEV" => "GOOG/NYSE",
"MDY" => "GOOG/NYSE",
"ZIV" => "GOOG/AMEX",
"EDV" => "GOOG/AMEX",
"SHY" => "GOOG/AMEX"
]

#quandl_basket = [
#"ZIV" => "GOOG/AMEX"
#]

portfolio = [collect(keys(quandl_basket))]
basket = Dict()
for stock in keys(quandl_basket)
     basket[stock] = quandl(string(quandl_basket[stock], "_", stock), order="des", rows=360)
end


basket_volatility1 = volatility("sliding", 3, 20, basket)
basket_volatility2 = volatility("grouping", 3, 20, basket)
basket_volatility3 = volatility("all", 3, 20, basket)

basket_performance1 = performance("open", 3, basket)
basket_performance2 = performance("close", 3, basket)
basket_performance3 = performance("openclose", 3, basket)
basket_performance4 = performance("closeopen", 3, basket)

basket_raw1 = raw_basket(basket_volatility1, basket_performance1)
basket_raw2 = raw_basket(basket_volatility1, basket_performance2)
basket_raw3 = raw_basket(basket_volatility1, basket_performance3)
basket_raw4 = raw_basket(basket_volatility1, basket_performance4)

basket_raw5 = raw_basket(basket_volatility2, basket_performance1)
basket_raw6 = raw_basket(basket_volatility2, basket_performance2)
basket_raw7 = raw_basket(basket_volatility2, basket_performance3)
basket_raw8 = raw_basket(basket_volatility2, basket_performance4)

basket_raw9 = raw_basket(basket_volatility3, basket_performance1)
basket_raw10 = raw_basket(basket_volatility3, basket_performance2)
basket_raw11 = raw_basket(basket_volatility3, basket_performance3)
basket_raw12 = raw_basket(basket_volatility3, basket_performance4)

basket_rank1 = rank_basket(basket_volatility1, basket_performance1)
basket_rank2 = rank_basket(basket_volatility1, basket_performance2)
basket_rank3 = rank_basket(basket_volatility1, basket_performance3)
basket_rank4 = rank_basket(basket_volatility1, basket_performance4)

basket_rank5 = rank_basket(basket_volatility2, basket_performance1)
basket_rank6 = rank_basket(basket_volatility2, basket_performance2)
basket_rank7 = rank_basket(basket_volatility2, basket_performance3)
basket_rank8 = rank_basket(basket_volatility2, basket_performance4)

basket_rank9 = rank_basket(basket_volatility3, basket_performance1)
basket_rank10 = rank_basket(basket_volatility3, basket_performance2)
basket_rank11= rank_basket(basket_volatility3, basket_performance3)
basket_rank12 = rank_basket(basket_volatility3, basket_performance4)

println(basket_rank1)
println(basket_rank2)
println(basket_rank3)
println(basket_rank4)
println(basket_rank5)
println(basket_rank6)
println(basket_rank7)
println(basket_rank8)
println(basket_rank9)
println(basket_rank10)
println(basket_rank11)
println(basket_rank12)
