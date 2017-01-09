module GeoCommands
    def geoadd(key, longitude, latitude, member, *llms)
        send_cluster_command([:geoadd, longitude, latitude, member, llms])
    end

    def geohash(key, member, *members)
        send_cluster_command([:geohash, key, member, members])
    end

    def geopos(key, member, *members)
        send_cluster_command([:geopos, key, member, members])
    end

    def geodist(key, member1, member2, unit = nil)
        send_cluster_command([:geodist, key, member1, member2, unit])
    end

    def georadius(key, longitude, latitude, radius, unit, options = {})
        args, store_key, store_dist_key = georadius_args(options)
        result = send_cluster_command([:georadius, key, longitude, latitude,
                                       radius, unit, args])
        georadius_store_result(result, store_key, store_dist_key)
        result
    end

    def georadiusbymember(key, member, radius, unit, options = {})
        args, store_key, store_dist_key = georadius_args(options)
        result =  send_cluster_command([:georadiusbymember, key, member,
                                        radius, unit, args])
        georadius_store_result(result, store_key, store_dist_key)
        result
    end

    private

    def flop_pairs(pairs)
        pairs.reduce([]) {|agg, it| agg << [it[1], it[0]] }
    end

    def georadius_args(options)
        args = []
        withcoord = options[:withcoord]
        args.concat(['WITHCOORD']) if withcoord

        withdist = options[:withdist]
        args.concat(['WITHDIST']) if withdist

        withhash = options[:withhash]
        args.concat(['WITHHASH']) if withhash

        count = options[:count]
        args.concat(['COUNT', count]) if count

        direction = options[:asc] ? 'ASC' :
                    options[:desc] ? 'DESC' :
                    nil
        args.concat([direction]) if direction

        store_key = options[:store]
        args.concat(['WITHHASH']) if withhash.nil? && store_key

        store_dist_key = options[:store_dist]
        args.concat(['WITHDIST']) if withdist.nil? && store_dist_key

        return args, store_key, store_dist_key
    end

    def georadius_store_result(result, store_key, store_dist_key)
        if store_key
            score_members = flop_pairs(result)
            send_cluster_command([:zadd, store_key, score_members])
        end

        if store_dist_key
            score_members = flop_pairs(result)
            send_cluster_command([:zadd, store_dist_key, score_members])
        end
    end
end
