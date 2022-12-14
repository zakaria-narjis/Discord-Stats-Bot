import discord as ds
import pandas as pd
from discord.ext import tasks
from discord.ext import commands
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
from time import process_time_ns
import time
import matplotlib.pyplot as plt
import re

#Connection to MangoDB:password defined in the variables environment
password = ''
CONNECTION_URL = "mongodb+srv://solos:"+password+"@botsolos.muidzcw.mongodb.net/test"
cluster = MongoClient(CONNECTION_URL)
database = cluster["Discord_statistics"]
collection = database["Statistics"]

#Discord bot intents(Important for working correctly with certain functions and accessing certain data)
intents = ds.Intents.all()
intents.members = True
intents.presences = True
client = commands.Bot(command_prefix='$$',intents=intents)

#Discord bot Token
Token = ''

#Formats timedelta object to dict form {hours:hh,minutes:mm,seconds:ss}
def format_timedelta(td:timedelta)->dict():
    minutes, seconds = divmod(td.seconds + td.days * 86400, 60)
    hours, minutes = divmod(minutes, 60)
    return {
        'hours':hours,
        'minutes':minutes,
        'seconds':seconds
    }

#Calculates duration between two datetime object and return dict in the form of {hours:hh,minutes:mm,seconds:ss}
def timedelta_calc(dt1:datetime,dt2=datetime)->dict():
    return format_timedelta(dt2-dt1)

#Calculates sum of two duration given in dictionary form {hours:hh,minutes:mm,seconds:ss}
def format_timedelta_calc(dt1:dict(),dt2:dict())->dict():
     t0 = timedelta(hours=dt1['hours'],minutes=dt1['minutes'],seconds=dt1['seconds'])
     t1 = timedelta(hours=dt2['hours'],minutes=dt2['minutes'],seconds=dt2['seconds'])
     return format_timedelta(t0+t1)

'''
insert_new_activity function:
Checks member's new activity
Insert activity data in the appropriate type of activities in the form of:
{
 'activity_type':{
                  new_activity_name:{
                                        'records':{'hours': 0, 'minutes': 0, 'seconds': 0}
                                        'previous_check_time':datetime.datetime or NULL
 WHERE:
     'records'            : Total time spent on the activity                                         
     'previous_check_time': Datetime when the bot last checked the user's activity
'''
def insert_new_activity(activity:ds.Activity,guild:ds.guild,update:str,member:ds.Member,doc:dict()):
    extracted_doc = doc['members'][str(member.id)]['activities'][activity.type.name]
    if activity.name not in extracted_doc:
        collection.update_one(
            {'_id': guild.id},
            {'$set': {update: {'records': {'hours': 0, 'minutes': 0, 'seconds': 0},
                               'previous_check_time': datetime.utcnow()}}}, upsert=True)
        if activity.type.name!='streaming':
            if activity.start is not None:
                collection.update_one(
                    {'_id': guild.id},
                    {'$set': {update + '.' + 'records': timedelta_calc(dt1=activity.start, dt2=datetime.utcnow())}},
                    upsert=True)
    else:
        if  extracted_doc[activity.name]['previous_check_time']==None:
            collection.update_one(
                {'_id': guild.id},
                {'$set': {update + '.' + 'previous_check_time': datetime.utcnow()}})

def time_dict_to_hour(dt=dict()):
    return timedelta(hours=dt['hours'], minutes=dt['minutes'], seconds=dt['seconds']).total_seconds()/3600


#Creates data document (JSON) for discord server/returns document dict
def document_init(guild:ds.guild)->dict():
    document = {
        '_id': guild.id,
        'name': guild.name,
        'members_count': guild.member_count,
        'members': {str(member.id): {
            '_id': member.id,
            'name': member.name,
            'discriminator': member.discriminator,
            'connection_record':{'records':{'hours': 0, 'minutes': 0, 'seconds': 0},'previous_check_time':None},
            'voice_com_record':{'records':{'hours': 0, 'minutes': 0, 'seconds': 0},'previous_check_time':None},
            'activities': {
                'playing': {},
                'streaming': {},
                'listening': {},
                'watching': {},
            }} for member in guild.members if member.bot == False},
        'voice_channels': {str(channel.id): {
            'channel_name': str(channel.name),
            'usage': {
                'hours': 0,
                'minutes': 0,
                'seconds': 0
            },
            'previous_check_time': None
        } for channel in guild.voice_channels},
        'recording_flag': True,
        'recording_start_time':datetime.now()
    }
    return document

@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))
    for guild in client.guilds:
        result = collection.find_one({'_id':guild.id})
        if result is None:
            collection.insert_one(document_init(guild))
    gathering_data.start()

@tasks.loop(seconds = 30) # repeats after every 10 seconds
async def gathering_data():
   print('[{time}]   Gathering_data...'.format(time=datetime.now()))
   for guild in client.guilds:
        proc_s = process_time_ns()
        elap_s = time.time()
        doc = collection.find_one({'_id': guild.id, })
        '''User's Voice activity'''
        for channel in guild.voice_channels:
            if  channel.members!=[]:
                for member in channel.members:
                    if member.bot!=True:
                        vc = member.voice
                        extracted_doc = doc['members'][str(member.id)]['voice_com_record']
                        update = 'members.' + str(member.id) + '.voice_com_record'
                        if not (vc.self_mute and vc.self_deaf) and  all(map(lambda x:not x, [vc.mute,vc.deaf,vc.suppress,vc.afk])):
                            pct = extracted_doc['previous_check_time']
                            if pct is None:
                                dt = timedelta_calc(dt1=datetime.utcnow(), dt2=datetime.utcnow())
                            else:
                                dt = timedelta_calc(dt1=pct, dt2=datetime.utcnow())
                            t0 = extracted_doc['records']
                            record = format_timedelta_calc(t0,dt)
                            collection.update_one(
                                {'_id':guild.id},
                                {'$set':{update:{'records':record,'previous_check_time': datetime.utcnow()}}})
                        else:
                            collection.update_one(
                                {'_id': member.guild.id},
                                {'$set': {update + '.previous_check_time': None}})
                '''Voice channel usage'''
                update='voice_channels.'+str(channel.id)
                pct=doc['voice_channels'][str(channel.id)]['previous_check_time']
                if pct is None:
                    dt = timedelta_calc(dt1=datetime.utcnow(), dt2=datetime.utcnow())
                else:
                    dt = timedelta_calc(dt1=pct, dt2=datetime.utcnow())
                t0 = doc['voice_channels'][str(channel.id)]['usage']
                collection.update_one({'_id':guild.id},
                                      {'$set':{
                                          update + '.usage': format_timedelta_calc(t0, dt),
                                          update+'.previous_check_time':datetime.utcnow()}})
            else:
                collection.update_one(
                            {'_id':guild.id},
                            {'$set':{'voice_channels.'+str(channel.id)+'.previous_check_time':None}})

        for member in guild.members :
            if member.bot==False:
                if str(member.status)!='offline':
                    '''User's connection status'''
                    update = 'members.' + str(member.id) +'.connection_record'
                    extracted_doc = doc['members'][str(member.id)]['connection_record']
                    pct = extracted_doc['previous_check_time']
                    if pct is None:
                        dt = timedelta_calc(dt1=datetime.utcnow(), dt2=datetime.utcnow())
                    else:
                        dt = timedelta_calc(dt1=extracted_doc['previous_check_time'],dt2=datetime.utcnow())
                    t0 = extracted_doc['records']
                    collection.update_one(
                        {'_id': guild.id},
                        {'$set': {update: {'records': format_timedelta_calc(t0, dt), 'previous_check_time': datetime.utcnow()}}})


                    '''User's activities'''

                    for activity in member.activities:
                        if activity.type.name in ['playing','streaming','listening','watching']:
                            update = 'members.' + str(member.id) + '.activities.' + activity.type.name + '.' + activity.name
                            insert_new_activity(activity,guild,update,member,doc)
                            doc=collection.find_one({'_id':guild.id,})
                            extracted_doc = doc['members'][str(member.id)]['activities'][activity.type.name][activity.name]
                            dt = timedelta_calc(dt1=extracted_doc['previous_check_time'],dt2=datetime.utcnow())
                            t0 = extracted_doc['records']
                            collection.update_one(
                                        {'_id':guild.id},
                                        {'$set':{update :{'records':format_timedelta_calc(t0,dt),'previous_check_time': datetime.utcnow()}}})

                else:

                    '''User's connection status'''
                    update = 'members.' + str(member.id) + '.connection_record'
                    collection.update_one(
                        {'_id': guild.id},
                        {'$set': {update+'.previous_check_time':None}})

        elap_e = time.time()
        proc_e = process_time_ns()
        print('[{guild}] SERVER :  CPU processing time :{time:.2f}ms     Total elapsed time:{time1:.2f}s'.format(guild=guild,time=(proc_e-proc_s)/1000000,time1=elap_e-elap_s))

@client.event
async def on_member_update(before,after):
    if before.bot==False:

        '''Checking Online/Offline status'''
        if after.status!=before.status:
            update = 'members.' + str(after.id) + '.connection_record'
            if str(after.status)=='offline':
                collection.update_one(
                    {'_id': after.guild.id},
                    {'$set': {update + '.previous_check_time': None}})
            else:
                collection.update_one(
                    {'_id': after.guild.id},
                    {'$set': {update + '.previous_check_time': datetime.utcnow()}})

        '''Checking activities'''
        for activity in before.activities:
            if activity not in after.activities and activity.type.name in ['playing','streaming','listening','watching']:
                update = 'members.' + str(before.id) + '.activities.' + str(activity.type.name) + '.' + activity.name
                collection.update_one(
                    {'_id': before.guild.id},
                    {'$set': {update+'.previous_check_time':None}}
                )
        for activity in after.activities:
            if activity not in before.activities and activity.type.name in ['playing','streaming','listening','watching']:
                update = 'members.' + str(before.id) + '.activities.' + str(activity.type.name) + '.' + activity.name
                doc = collection.find_one({'_id': after.guild.id })
                insert_new_activity(activity, before.guild, update,after,doc)
                collection.update_one(
                    {'_id': before.guild.id},
                    {'$set': {update+'.previous_check_time':datetime.utcnow()}})

@client.event
async def on_voice_state_update(member,before,after):
    if member.bot==False:
        update = 'members.' + str(member.id) + '.voice_com_record'
        if (after.channel is not None) and (before is None) :
            if str(after.channel.type) == 'voice':
                collection.update_one(
                    {'_id': member.guild.id},
                    {'$set': {update + '.previous_check_time':datetime.utcnow()}})
        elif (after.channel is None) and (before is not None):
            if str(before.channel.type) == 'voice':
                collection.update_one(
                    {'_id': member.guild.id},
                    {'$set': {update + '.previous_check_time': None}})

@client.event
async def on_disconnect():
    print('DISCONNECTED...')

@client.event
async def on_member_join(member):
    if member.bot==False:
        update='members.' + str(member.id)
        print(member.name+'joined')
        collection.update_one(
            {'_id': member.guild.id, },
            {'$set':{update:{
                '_id': member.id,
                'name': member.name,
                'discriminator': member.discriminator,
                'connection_record':{'records':{'hours': 0, 'minutes': 0, 'seconds': 0},'previous_check_time':None},
                'voice_com_record':{'records':{'hours': 0, 'minutes': 0, 'seconds': 0},'previous_check_time':None},
                'activities': {
                    'playing': {},
                    'streaming': {},
                    'listening': {},
                    'watching': {},
                }}}}
        )

@client.event
async def on_user_update(before,after):
    if after.bot == False:
        if before.discriminator!=after.discriminator:
            update='members.'+str(before.id)+'.discriminator'
            collection.update_many(
                {'members.'+str(before.id)+'._id': before.id},
                {'$set': {update:after.discriminator}})
        if before.name!=after.name:
            update='members.'+str(before.id)+'.name'
            c=collection.update_many(
                {'members.'+str(before.id)+'._id': before.id},
                {'$set': {update:after.name}})

@client.command()
async def stats(ctx,name_disc):
    member=ctx.guild.get_member_named(name_disc)
    doc = collection.find_one(
        {'_id':ctx.guild.id},)
    extracted_doc=doc['members'][str(member.id)]
    message = member.name+' statistics :'
    cr=extracted_doc['connection_record']['records']
    message+='\nOnline_time: {hours}h, {minutes}m, {seconds}s'.format(hours=cr['hours'],minutes=cr['minutes'],seconds=cr['seconds'])
    vr = extracted_doc['voice_com_record']['records']
    message+='\nVoice_chat_time: {hours}h, {minutes}m, {seconds}s'.format(hours=vr['hours'],minutes=vr['minutes'],seconds=vr['seconds'])
    ac=extracted_doc['activities']
    for activity_type in ac:
        if ac[activity_type]!= {}:
            message+='\n' + activity_type+':'
            for activity in ac[activity_type]:
                rs=ac[activity_type][activity]['records']
                message+='\n '+ 'Total time spent on '+ activity+':'+' {hours}h, {minutes}m, {seconds}s'.format(hours=rs['hours'],minutes=rs['minutes'],seconds=rs['seconds'])
    await ctx.send(message)

@client.command()
async def top_online(ctx,num='10',tp='txt'):
    if num.isnumeric():
        num=int(num)
        if  num<=10 and num>0:
            doc = collection.find_one(
                {'_id':ctx.guild.id},)
            data={
                    "".join(re.findall("[a-zA-Z]+", doc['members'][member_id]['name']))[:15]:time_dict_to_hour(doc['members'][member_id]['connection_record']['records']) for member_id in doc['members']
                }
            ser = pd.Series(data=data).nlargest(n=num)
            if tp=='txt':
                message=''
                for key,value,i in zip(ser.keys(), ser.values,range(1,num+1)):
                    message+='\nRANK {n}: {member} {time:.2f} hours'.format(n=i,member=key,time=value)
                await ctx.send(message)
            if tp=='graph':
                fig, ax = plt.subplots(figsize=(15, 13), constrained_layout=True)
                ax.bar(ser.keys(), ser.values)
                ax.set_xlabel('Hours')
                ax.set_ylabel('Members')
                ax.set_title('TOP '+str(num)+' Online members')
                fig.savefig('top_n_graph.png')
                chart=ds.File('top_n_graph.png')
                await ctx.send(file=chart)
    else:
        await ctx.send('WRONG ARGUMENTS.')
        
@client.command()
async def top_voice(ctx,num='10',tp='txt'):
    if num.isnumeric():
        num=int(num)
        if  num<=10 and num>0:
            doc = collection.find_one(
                {'_id':ctx.guild.id},)
            data={
                    "".join(re.findall("[a-zA-Z]+", doc['members'][member_id]['name']))[:15]:time_dict_to_hour(doc['members'][member_id]['voice_com_record']['records']) for member_id in doc['members']
                }
            ser = pd.Series(data=data).nlargest(n=num)
            if tp=='txt':
                message=''
                for key,value,i in zip(ser.keys(), ser.values,range(1,num+1)):
                    message+='\nRANK {n}: {member} {time:.2f} hours'.format(n=i,member=key,time=value)
                await ctx.send(message)
            if tp=='graph':
                fig, ax = plt.subplots(figsize=(15, 13), constrained_layout=True)
                ax.bar(ser.keys(), ser.values)
                ax.set_ylabel('Hours')
                ax.set_xlabel('Members')
                ax.set_title('TOP '+str(num)+' voice chat users')
                fig.savefig('top_n_graph.png')
                chart=ds.File('top_n_graph.png')
                await ctx.send(file=chart)
        else:
            await ctx.send('THE NUMBER MUST BE BETWEEN 1 AND 10.')
    else:
        await ctx.send('WRONG ARGUMENTS.')

client.run(Token)
