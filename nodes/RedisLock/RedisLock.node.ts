import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription, NodeConnectionType,
} from 'n8n-workflow';
import { NodeOperationError } from 'n8n-workflow';

import type { RedisCredential } from './types';
import {
	setupRedisClient,
	redisConnectionTest,
	getValue,
	setValue,
} from './utils';

export class RedisLock implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'RedisLock',
		name: 'redisLock',
		icon: 'file:redis.svg',
		group: ['input'],
		version: 1,
		description: 'Get, send and update data in Redis',
		defaults: {
			name: 'RedisLock',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main, NodeConnectionType.Main],
		outputNames: ['unlocked', 'locked'],
		usableAsTool: true,
		credentials: [
			{
				name: 'redisLock',
				required: true,
				testedBy: 'redisConnectionTest',
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Lock',
						value: 'lock',
						description: 'Lock by key',
						action: 'Set the lock in redis',
					},
				],
				default: 'lock',
			},

			// ----------------------------------
			//         lock
			// ----------------------------------
			{
				displayName: 'Key',
				name: 'key',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['lock'],
					},
				},
				default: '',
				required: true,
				description: 'Name of the lock key to set in Redis',
			},
			{
				displayName: 'Expire',
				name: 'expire',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['lock'],
					},
				},
				default: true,
				description: 'Whether to set a timeout on lock key',
			},

			{
				displayName: 'TTL',
				name: 'ttl',
				type: 'number',
				typeOptions: {
					minValue: 1,
				},
				displayOptions: {
					show: {
						operation: ['lock'],
						expire: [true],
					},
				},
				default: 60,
				description: 'Number of seconds before lock expiration',
			},

			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				displayOptions: {
					show: {
						operation: ['lock'],
					},
				},
				placeholder: 'Add option',
				default: {},
				options: [
					{
						displayName: 'Dot Notation',
						name: 'dotNotation',
						type: 'boolean',
						default: true,
						// eslint-disable-next-line n8n-nodes-base/node-param-description-boolean-without-whether
						description:
							'<p>By default, dot-notation is used in property names. This means that "a.b" will set the property "b" underneath "a" so { "a": { "b": value} }.<p></p>If that is not intended this can be deactivated, it will then set { "a.b": value } instead.</p>.',
					},
				],
			},
		],
	};

	methods = {
		credentialTest: { redisConnectionTest },
	};

	async execute(this: IExecuteFunctions) {
		const credentials = await this.getCredentials<RedisCredential>('redis');

		const client = setupRedisClient(credentials);
		await client.connect();
		await client.ping();

		const operation = this.getNodeParameter('operation', 0);

		const unlockedItems: INodeExecutionData[] = [];
		const lockedItems: INodeExecutionData[] = [];

		if (operation === 'lock') {
			const items = this.getInputData();

			let item: INodeExecutionData;
			for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
				item = { json: {}, pairedItem: { item: itemIndex } };
				try {
					const keyGet = this.getNodeParameter('key', itemIndex) as string;

					const value = (await getValue(client, keyGet, 'string')) ?? null;

					if (value) {
						lockedItems.push(item)
					} else {
						const keySet = this.getNodeParameter('key', itemIndex) as string;
						const expire = this.getNodeParameter('expire', itemIndex, false) as boolean;
						const ttl = this.getNodeParameter('ttl', itemIndex, -1) as number;
						await setValue.call(this, client, keySet, 'locked', expire, ttl, 'string', false);
						unlockedItems.push(item)
					}
				} catch (error) {
					if (this.continueOnFail()) {
						lockedItems.push(item);
						continue;
					}
					await client.quit();
					throw new NodeOperationError(this.getNode(), error, {itemIndex});
				}
			}
		}
		await client.quit();
		return [unlockedItems, lockedItems];
	}
}
